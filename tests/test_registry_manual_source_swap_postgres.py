# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual imported-model publication preserves manual audit and pinned registry reads."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from process import attributes, initial
from process.reference_family_result_generation import RELATION_NAMES_BY_IMPORTER
from process.registry_approved_company_read import read_registry_approved_companies
from process.registry_issuer_resolution import read_registry_issuer_resolutions
from process.registry_management_permissions import verify_registry_management_permissions
from process.registry_record_store import get_registry_record
from process.registry_source_admission import admit_cms_mlr_edition
from tests.registry_manual_source_swap_fixture import manual_source_swap_setup
from tests.test_company_registry_assertions_postgres import (
    _approve,
    _create,
    _declared_group,
    _draft,
    _durable_manual_state,
    _role,
    _with_groups,
    cms_edition,
    cms_input,
    group_command,
)
from tests.test_company_registry_assertions_postgres import (
    assertion_db as assertion_db,
)

pytestmark = pytest.mark.asyncio
_DURABLE = (
    "company_registry",
    "company_group_registry",
    "company_registry_role_assertion",
    "company_registry_identifier_assertion",
    "company_registry_group_assertion",
    "registry_record_history",
    "registry_approved_record",
    "registry_revision_control",
)


async def _manual_composition(fixture):
    """Build real corrected and jointly approved manual company/group evidence."""
    group_creation = group_command()
    group_creation.fields.update(aliases=["Manual parent alias"])
    group = await _draft(fixture, group_creation)
    creation = _create()
    creation.fields.update(roles=["employer", "network_operator"], aliases=["Original company alias"])
    creation.fields["assertions"]["role_assertions"].append(_role() | {"role": "network_operator"})
    creation = _with_groups(creation, [_declared_group(creation.record_id, group_creation.record_id)])
    await _draft(fixture, creation)
    corrected = await _draft(
        fixture,
        replace(
            creation,
            operation="correct",
            expected_revision=1,
            idempotency_key="swap-correction",
            fields=creation.fields
            | {
                "aliases": ["Corrected company alias"],
                "assertions": creation.fields["assertions"] | {"expected_revision": 1},
            },
        ),
    )
    approved = await _approve(fixture, group, corrected)
    input_bytes = cms_input(company_name=corrected["record"]["display_name"])
    async with fixture.connection.transaction():
        admitted = await admit_cms_mlr_edition(
            fixture.connection,
            input_bytes,
            cms_edition(input_bytes),
            control_schema=fixture.schema,
        )
    assert admitted["resolved_issuers"] == 2
    return group_creation.record_id, creation.record_id, approved["approved_revision"]


async def _oids(connection, schema, names):
    """Read actual relation identities in one bounded catalog statement."""
    records = await connection.fetch(
        "SELECT name,to_regclass(format('%I.%I',$1::text,name))::oid::bigint AS oid "
        "FROM unnest($2::text[]) name ORDER BY name",
        schema,
        list(names),
    )
    oid_by_name = {record["name"]: record["oid"] for record in records}
    assert len(oid_by_name) == len(names) and all(oid is not None for oid in oid_by_name.values())
    return oid_by_name


async def _generation_rows(connection, namespace):
    records = await connection.fetch(
        f"SELECT importer_id,local_generation,relation_oids,source_revision_tracked "
        f"FROM {namespace}.reference_family_result_generation ORDER BY importer_id"
    )
    return {record["importer_id"]: dict(record) for record in records}


async def _pinned_state(session, fixture, group_id, company_id, revision):
    """Read the actual registry projection and typed heads on one native snapshot."""
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    approved = await read_registry_approved_companies(driver, schema=fixture.schema, approved_revision=revision)
    issuers = await read_registry_issuer_resolutions(driver, ("00123", "00124"), control_schema=fixture.schema)
    heads = (
        await get_registry_record(session, "group", group_id, schema=fixture.schema),
        await get_registry_record(session, "company", company_id, schema=fixture.schema),
    )
    assert len(issuers) == 2 and all(issuer["legal_company"]["company_id"] != str(company_id) for issuer in issuers)
    assert str(company_id) in {item["company_id"] for item in approved["items"]}
    assert all(head is not None for head in heads)
    return approved, issuers, heads


async def _seed_markers(fixture, stages, family):
    """Seed distinct imported values through the genuine model columns."""
    marker_table = "issuer" if family == "mrf" else "plan_attributes"
    stage_table = next(stage.__tablename__ for stage in stages if stage.__main_table__ == marker_table)
    if family == "mrf":
        for table, label in ((marker_table, "Old imported issuer"), (stage_table, "New imported issuer")):
            await fixture.connection.execute(
                f'INSERT INTO {fixture.namespace}."{table}"(issuer_id,state,issuer_name) VALUES(123,$2,$1)',
                label,
                "CA",
            )
    else:
        for table, label in ((marker_table, "Old imported attribute"), (stage_table, "New imported attribute")):
            await fixture.connection.execute(
                f'INSERT INTO {fixture.namespace}."{table}"(full_plan_id,year,attr_name,attr_value,plan_id) '
                "VALUES('12345CA0010001-01',2024,'EvidenceMarker',$1,'12345CA0010001')",
                label,
            )


async def _verify_rotation(fixture, stages, family, live_before, stage_before, generations_before):
    """Verify real relation identities, generation accounting and new imported values."""
    live_names = tuple(stage.__main_table__ for stage in stages)
    live_after = await _oids(fixture.connection, fixture.schema, live_names)
    assert live_after == {stage.__main_table__: stage_before[stage.__tablename__] for stage in stages}
    assert all(live_after[name] != live_before[name] for name in live_names)
    old_oids = await _oids(fixture.connection, fixture.schema, tuple(name + "_old" for name in live_names))
    assert old_oids == {name + "_old": oid for name, oid in live_before.items()}
    generations_after = await _generation_rows(fixture.connection, fixture.namespace)
    selected = {"mrf", "mrf-address"} if family == "mrf" else {"plan-attributes"}
    for importer, before in generations_before.items():
        after = generations_after[importer]
        if importer in selected:
            assert after["local_generation"] == before["local_generation"] + 1
            assert after["source_revision_tracked"] is True
            assert tuple(after["relation_oids"]) == tuple(
                live_after[name] for name in RELATION_NAMES_BY_IMPORTER[importer]
            )
        else:
            assert after == before
    marker_table = "issuer" if family == "mrf" else "plan_attributes"
    column = "issuer_name" if family == "mrf" else "attr_value"
    label = await fixture.connection.fetchval(f'SELECT {column} FROM {fixture.namespace}."{marker_table}"')
    assert label == ("New imported issuer" if family == "mrf" else "New imported attribute")


async def _exercise_publication(fixture, monkeypatch, family):
    """Execute the real publisher while an independent durable Reader remains pinned."""
    extension_rows = await fixture.connection.fetch("SELECT extname FROM pg_extension")
    # Required by the real GIN scalar/trigram indexes.
    assert {"pg_trgm", "btree_gin"} <= {extension_row["extname"] for extension_row in extension_rows}
    group_id, company_id, revision = await _manual_composition(fixture)
    manual_before = await _durable_manual_state(fixture, group_id, company_id, revision)
    durable_oids = await _oids(fixture.connection, fixture.schema, _DURABLE)
    async with manual_source_swap_setup(fixture, monkeypatch) as setup:
        stages = setup.mrf_stages if family == "mrf" else setup.attribute_stages
        live_names = tuple(stage.__main_table__ for stage in stages)
        stage_names = tuple(stage.__tablename__ for stage in stages)
        live_before = await _oids(fixture.connection, fixture.schema, live_names)
        stage_before = await _oids(fixture.connection, fixture.schema, stage_names)
        generations_before = await _generation_rows(fixture.connection, fixture.namespace)
        await _seed_markers(fixture, stages, family)
        async with fixture.sessions() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            reader_driver = (await (await reader.connection()).get_raw_connection()).driver_connection
            assert await reader_driver.fetchval("SELECT current_user") == fixture.roles_by_kind["draft"]
            await verify_registry_management_permissions(reader_driver, **fixture.permission_arguments())
            pinned_before = await _pinned_state(reader, fixture, group_id, company_id, revision)
            if family == "mrf":
                published = await initial._publish_mrf_table_generation(setup.import_id, fixture.schema)
                assert published.local_generation == 1
            else:
                await attributes.finalize_attribute_tables(setup.attribute_context)
                assert setup.attribute_context["context"]["tables_prepared"] is False
            assert await _pinned_state(reader, fixture, group_id, company_id, revision) == pinned_before
            assert await _oids(fixture.connection, fixture.schema, _DURABLE) == durable_oids
            await _verify_rotation(fixture, stages, family, live_before, stage_before, generations_before)

        # Reuse the existing draft connection after its pinned Reader releases it.
        assert await _durable_manual_state(fixture, group_id, company_id, revision) == manual_before


async def test_manual_approved_state_and_pinned_registry_reader_survive_real_mrf_rotation(assertion_db, monkeypatch):
    await _exercise_publication(assertion_db, monkeypatch, "mrf")


async def test_manual_approved_state_and_pinned_registry_reader_survive_aca_attribute_publication(
    assertion_db, monkeypatch
):
    await _exercise_publication(assertion_db, monkeypatch, "plan-attributes")
