# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scheduling observes genuine protected PTG metadata without replaying graph facts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from process import registry_membership_refresh_ptg_observation as observation
from process import registry_ptg_graph_reader as graph
from process import registry_ptg_office_witness as witness
from process.registry_company_approval_fence import registry_company_approval_transaction
from tests.test_registry_ptg_refresh_anchors_postgres import (
    _native_binding,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    cms_recipe_database as cms_recipe_database,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    cms_recipe_template as cms_recipe_template,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    coordinator_db as coordinator_db,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    mixed_ptg_custody as mixed_ptg_custody,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    office_db as office_db,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    office_publisher as office_publisher,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    reviewed_cms_source as reviewed_cms_source,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    reviewed_ptg_offices as reviewed_ptg_offices,
)
from tests.test_registry_ptg_refresh_anchors_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio


async def test_observer_rechecks_native_capture_but_never_reads_graph_or_office_facts(mixed_ptg_custody, monkeypatch):
    case = mixed_ptg_custody
    forbidden = AsyncMock(side_effect=AssertionError("scheduling read reached source facts"))
    monkeypatch.setattr(graph, "verify_registry_ptg_graph_page", forbidden)
    monkeypatch.setattr(witness, "_office_rows", forbidden)
    readers = SimpleNamespace(control_schema=case.fixture.control_schema, ptg_store=case.config.ptg.store)
    async with registry_company_approval_transaction(
        case.producer_sessions, control_schema=readers.control_schema
    ) as session:
        binding, approved = await _native_binding(session, case)
        before = await session.scalar(text("SELECT current_setting('search_path')"))
        scopes, recipes = await observation.observe_accepted_ptg_recipes(
            session, (binding,), approved, readers, graph.RegistryPTGGraphReadBudget(16 * 1024 * 1024)
        )
        assert recipes == (case.ptg_recipe,)
        assert scopes[binding.binding_id]["capture_generation"] == case.ptg_recipe.source_pin.generation_id
        assert scopes[binding.binding_id]["approved_binding_sha256"] == binding.approved_binding_sha256
        assert await session.scalar(text("SELECT current_setting('search_path')")) == before
        forbidden.assert_not_awaited()
