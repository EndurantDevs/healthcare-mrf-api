# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded native producer SQL and packed graph checks, below service authority.

The existing fixture owns real retained publication, drafts, approvals and exact
cleanup. These cases do not establish installed published-plan service or HTTP
acceptance; they exercise the new set query and producer using native stores.
"""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models import PTG2PriceAtom
from process import registry_ptg_office_capture as capture
from process import registry_ptg_published_office_scope as published
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_preparation_producer import produce_reviewed_office_rows
from process.registry_record_store import RegistryRecordCommand
from tests.test_network_approved_source_bindings_postgres import _apply, _binding
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create
from tests.test_registry_ptg_office_capture_postgres import complete_office_db as office_db
from tests.test_registry_ptg_office_capture_postgres import serving_schema as serving_schema
from tests.test_registry_published_plan_binding_refs import reviewed_binding
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
        assert office_rows[0]["source_record_ordinal"] == occurrence["source_record_ordinal"]
        changed = deepcopy(selector_by_field)
        changed["selections"][0]["source_record_ordinal"] = 2**63 - 1
        with pytest.raises(ValueError, match="selection_unresolved"):
            await produce_reviewed_office_rows(
                session, fixture.context, scope_by_field, changed, RegistryPTGGraphReadBudget(64 * 1024 * 1024)
            )

        changed = deepcopy(selector_by_field)
        changed["selections"].append({**changed["selections"][0], "binding_id": str(uuid4())})
        with pytest.raises(ValueError, match="selection_unresolved"):
            await produce_reviewed_office_rows(
                session, fixture.context, scope_by_field, changed, RegistryPTGGraphReadBudget(64 * 1024 * 1024)
            )


async def _approved_published_mapping(fixture):
    """Seed the retained reviewed map; upstream review authentication is separate."""
    actor = _actor()
    network = await _draft(fixture, _create("network"), actor)
    mapping, _ = reviewed_binding()
    mapping["network_id"] = int(network["record_id"])
    coordinates = RegistryNetworkSourceCoordinates(
        **{
            name: mapping[name]
            for name in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        }
    )
    # Use the real batch writer/approval before replacing only the retained scope
    # with this test's published-review fixture, below authenticated review intake.
    binding = _binding(
        int(network["record_id"]),
        **{
            name: getattr(coordinates, name)
            for name in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        },
    )
    receipt = await _apply(fixture.connection, fixture.control_schema, actor, [binding])
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, network, *receipt["records"]),
        actor,
    )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_approved_record SET record_json=record_json||$1::jsonb '
        "WHERE approved_revision=$2 AND record_kind='network_binding'",
        json.dumps(mapping),
        approval["approved_revision"],
    )
    context = SimpleNamespace(control_schema=fixture.control_schema, coordinates=coordinates)
    scope_by_field = {
        "source_scope": mapping["source_scope_json"],
        "binding_source_key": mapping["source_key"],
        "network_id": int(network["record_id"]),
    }
    return context, scope_by_field, approval["approved_revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "coordinate", [None, "source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"]
)
async def test_native_published_mapping_never_reuses_same_label_across_coordinates(custom_db, coordinate):
    context, scope, revision = await _approved_published_mapping(custom_db)
    async with async_sessionmaker(custom_db.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        assert await published._require_network_binding(session, context, scope) == revision
        if coordinate:
            context.coordinates = replace(
                context.coordinates, **{coordinate: "aca" if coordinate == "source_system" else "other_scope"}
            )
            with pytest.raises(ValueError, match="network_unresolved"):
                await published._require_network_binding(session, context, scope)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["scope", "source", "network", "missing", "conflicting", "evidence"])
async def test_native_published_mapping_refuses_missing_or_conflicting_review(custom_db, change):
    context, scope, revision = await _approved_published_mapping(custom_db)
    match change:
        case "scope":
            scope["source_scope"] = {**scope["source_scope"], "snapshot_id": "other_snapshot"}
        case "source":
            scope["binding_source_key"] = "other_source"
        case "network":
            scope["network_id"] += 1
        case "evidence":
            await custom_db.connection.execute(
                f'UPDATE "{custom_db.control_schema}".registry_approved_record SET record_json=record_json||$1::jsonb '
                "WHERE approved_revision=$2 AND record_kind='network_binding'",
                json.dumps({"evidence_sha256": "d" * 64}),
                revision,
            )
        case "missing":
            await custom_db.connection.execute(
                f'DELETE FROM "{custom_db.control_schema}".registry_approved_record '
                "WHERE approved_revision=$1 AND record_kind='network_binding'",
                revision,
            )
        case "conflicting":
            await custom_db.connection.execute(
                f'INSERT INTO "{custom_db.control_schema}".registry_approved_record '
                "SELECT approved_revision,record_kind,$1,record_revision,custom_revision,record_json "
                f'FROM "{custom_db.control_schema}".registry_approved_record '
                "WHERE approved_revision=$2 AND record_kind='network_binding'",
                str(uuid4()),
                revision,
            )
    async with async_sessionmaker(custom_db.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        with pytest.raises(ValueError, match="network_unresolved"):
            await published._require_network_binding(session, context, scope)


@pytest.mark.asyncio
async def test_native_price_only_atom_survives_missing_directory_mapping(custom_db):
    context, scope, revision = await _approved_published_mapping(custom_db)
    price_table = PTG2PriceAtom.__table__.to_metadata(MetaData(), schema=custom_db.source_schema)
    async with custom_db.engine.begin() as setup:
        await setup.run_sync(price_table.create)
        await setup.execute(price_table.insert(), {"price_atom_hash": "f" * 64, "negotiated_rate": "123.45"})
    await custom_db.connection.execute(
        f'DELETE FROM "{custom_db.control_schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND record_kind='network_binding'",
        revision,
    )
    async with async_sessionmaker(custom_db.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        before = (await session.execute(price_table.select())).mappings().all()
        with pytest.raises(ValueError, match="network_unresolved"):
            await published._require_network_binding(session, context, scope)
        after = (await session.execute(price_table.select())).mappings().all()
        assert before == after and after[0]["negotiated_rate"] == "123.45"
        assert (
            await session.scalar(
                text(
                    f'SELECT count(*) FROM "{custom_db.control_schema}".registry_approved_record '
                    "WHERE record_kind='membership'"
                )
            )
            == 0
        )
