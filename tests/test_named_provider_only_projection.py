# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed provider evidence requires complete mappings and the approved generation."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from process import network_provider_only_projection as projection
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_source_witness_contract import source_origin
from process.network_fhir_typed_contract import TypedFHIRGraphError, TypedFHIRGraphStage, graph_document
from process.network_fhir_typed_mapping import TypedGraphMapping
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_typed_source_stage import TypedOfficeMatches
from tests.test_named_fhir_typed_graph import COUNTS
from tests.test_network_fhir_source_witness_contract import origin_fields


@pytest.fixture
def mapped_stage():
    """Declare two named mappings and complete Source data documents."""
    approved = ApprovedMembershipSource(7, "c" * 64)
    address = PinnedAddressSource("address_generation", "offices", "d" * 64)
    mappings = []
    for label in ("first", "second"):
        source_stage = TypedFHIRGraphStage(
            {kind: label + "_" + kind for kind in ("cells", "addresses", "pairs")},
            schema_name="published_generation",
            document=graph_document(source_origin(origin_fields()), "2026-01-01", COUNTS),
            checked=True,
        )
        names_by_kind = {
            kind: label + "_" + kind
            for kind in ("provider_only", "offices", "memberships", "bindings", "office_keys", "office_matches")
        }
        mappings.append(TypedGraphMapping(source_stage, names_by_kind, {}))
    matches = TypedOfficeMatches("typed_offices", "e" * 64, approved.generation_id, address, (), tuple(mappings))
    stage = RegistrySourceRecipeStage(
        "typed_memberships",
        2,
        0,
        "f" * 64,
        contribution_revision=4,
        contribution_receipts=tuple({"recipe_sha256": identity * 64} for identity in ("b", "a")),
        office_matches=matches,
    )
    return stage, approved


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["ready", "wrong_approval", "incomplete_source", "mapping_tables"])
async def test_typed_provider_projection_checks_readiness(mapped_stage, state):
    """Check every mapping before copying its complete provider evidence columns."""
    stage, approved = mapped_stage
    if state == "wrong_approval":
        approved = ApprovedMembershipSource(8, "1" * 64)
    elif state == "incomplete_source":
        stage.office_matches.mappings[1].source_stage.checked = False
    elif state == "mapping_tables":
        stage.office_matches.mappings[1].table_names.pop("office_keys")
    connection = SimpleNamespace(execute=AsyncMock())
    if state == "ready":
        identity = await projection._append_typed_staged_sources(
            connection, '"expected_providers"', (), stage, approved, "controls"
        )
        assert identity == hashlib.sha256(("a" * 64 + "b" * 64).encode()).hexdigest()
        assert connection.execute.await_args_list == [
            call(
                f'INSERT INTO "expected_providers"({projection.COLUMNS}) SELECT {projection.COLUMNS} '
                f'FROM pg_temp."{label}_provider_only" ON CONFLICT DO NOTHING'
            )
            for label in ("first", "second")
        ]
    else:
        refusals_by_state = {
            "wrong_approval": "office_context",
            "incomplete_source": "source_incomplete",
            "mapping_tables": "mapping_tables",
        }
        with pytest.raises(TypedFHIRGraphError, match=refusals_by_state[state]):
            await projection._append_typed_staged_sources(
                connection, '"expected_providers"', (), stage, approved, "controls"
            )
        connection.execute.assert_not_awaited()
