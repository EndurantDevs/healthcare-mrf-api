# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded native producer SQL and packed graph checks, below service authority.

The existing fixture owns real retained publication, drafts, approvals and exact
cleanup. These cases do not establish installed published-plan service or HTTP
acceptance; they exercise the new set query and producer using native stores.
"""

import hashlib
import json
from copy import deepcopy
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import registry_ptg_office_capture as capture
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_preparation_producer import produce_reviewed_office_rows
from process.registry_record_store import RegistryRecordCommand
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_registry_approval_store_postgres import _actor, _approve, _command
from tests.test_registry_ptg_office_capture_postgres import complete_office_db as office_db
from tests.test_registry_ptg_office_capture_postgres import serving_schema as serving_schema
from tests.test_registry_site_membership_composition_postgres import _draft


async def _approved_site(fixture, custom):
    """Create and approve exactly one retained binding through the real store."""
    site_by_field = dict(
        await fixture.connection.fetchrow(
            f'''SELECT binding.provider_system,binding.provider_id,binding.location_id::text,binding.location_key,
          encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
          FROM "{fixture.serving.schema_name}".provider_location_binding binding
          JOIN "{fixture.serving.schema_name}".entity_address_unified address USING(location_key)
          ORDER BY binding.location_key LIMIT 1'''
        )
    )
    actor = _actor()
    command = RegistryRecordCommand(
        "site_binding",
        uuid4(),
        "create",
        0,
        {"source_generation": fixture.serving.generation_id, **site_by_field},
        "Reviewed this exact retained site_by_field",
        uuid4().hex,
    )
    binding = await _draft(custom, command, actor)
    approval = await _approve(
        custom.connection,
        custom.control_schema,
        await _command(custom.connection, custom.control_schema, binding),
        actor,
    )
    return site_by_field, binding, approval


@pytest.mark.asyncio
async def test_native_producer_selects_one_reviewed_site(office_db, custom_db):
    fixture = office_db
    site_by_field, binding, approval = await _approved_site(fixture, custom_db)
    scope_by_field = {**fixture.scope, "approved_revision": approval["approved_revision"]}
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        occurrence = (
            (
                await session.execute(
                    text(
                        f'''SELECT source_key,source_record_ordinal
              FROM "{fixture.context.source_specification.ptg_schema_name}".ptg2_provider_group_tax_identity_source
              WHERE snapshot_key=:key AND source_key=0 ORDER BY source_record_ordinal LIMIT 1'''
                    ),
                    {"key": fixture.context.graph_identity["snapshot_key"]},
                )
            )
            .mappings()
            .one()
        )
        selector_by_field = {
            "capture_id": str(uuid4()),
            "scope_id": scope_by_field["scope_id"],
            "client_id": scope_by_field["client_id"],
            "scope_approval_sha256": scope_by_field["approval_sha256"],
            "retained_generation_id": fixture.serving.generation_id,
            "reason": "Reviewed this exact retained site_by_field",
            "idempotency_key": uuid4().hex,
            "selections": [
                {
                    "binding_id": binding["record_id"],
                    "binding_revision": binding["revision"],
                    "dense_source_key": occurrence["source_key"],
                    "source_record_ordinal": occurrence["source_record_ordinal"],
                }
            ],
        }
        raw, digest, count = await produce_reviewed_office_rows(
            session, fixture.context, scope_by_field, selector_by_field, RegistryPTGGraphReadBudget(64 * 1024 * 1024)
        )
        assert count == 1 and digest == hashlib.sha256(capture._canonical(json.loads(raw)[0]) + b"\n").hexdigest()
        office_rows = json.loads(raw)
        assert office_rows[0]["location_id"] == site_by_field["location_id"]
        assert office_rows[0]["office_evidence_kind"] == "reviewed_exact_office"
        changed = deepcopy(selector_by_field)
        changed["selections"][0]["source_record_ordinal"] = 2**63 - 1
        with pytest.raises(ValueError, match="selection_unresolved"):
            await produce_reviewed_office_rows(
                session, fixture.context, scope_by_field, changed, RegistryPTGGraphReadBudget(64 * 1024 * 1024)
            )
