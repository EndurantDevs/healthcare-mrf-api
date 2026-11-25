# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported visibility, manual policy and atomic publication pairing on native SQL."""

import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import dialect

from api import network_address_scope as address_scope
from api.provider_detail_network_read import _selected_networks
from process import registry_effective_network_catalog as effective
from process import registry_network_catalog_read as catalog
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.effective_catalog_support import (
    GENERATION,
    MANIFEST,
    manual_record,
)
from tests.effective_catalog_support import (
    effective_catalog_db as effective_catalog_db,
)

pytestmark = pytest.mark.asyncio


def _plan_nodes(plan):
    yield plan
    for child in plan.get("Plans", ()):
        yield from _plan_nodes(child)


async def test_imported_network_visible_without_any_manual_approval(effective_catalog_db):
    db = effective_catalog_db
    page = await catalog._page(db.connection, db.manifest, catalog.RegistryNetworkCatalogQuery(), (), db.control)
    assert page["total"] == 1 and page["items"][0]["network_id"] == 7
    assert page["items"][0]["benefit_codes"] == ["medical"]
    assert page["items"][0]["source_bindings"][0]["source_id"] == "source-one"
    assert await _selected_networks(
        db.connection, (7,), db.manifest, (), db.control, RegistryOrganizationVisibility()
    ) == (7,)
    hidden = await catalog._page(db.connection, db.manifest, catalog.RegistryNetworkCatalogQuery(), (7,), db.control)
    assert hidden["total"] == 0
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await _selected_networks(db.connection, (7,), db.manifest, (7,), db.control, None)
    assert await db.connection.fetchval(f'SELECT count(*) FROM "{db.control}".registry_approved_record') == 0


async def test_native_catalog_list_pages_match_full_catalog(effective_catalog_db, monkeypatch):
    fixture = effective_catalog_db
    await manual_record(
        fixture, "network", 8, {"network_id": 8, "display_name": "A Manual", "aliases": ["PPO%"], "archived": False}
    )
    await manual_record(
        fixture, "network", 10, {"network_id": 10, "display_name": "PPO% Archived", "aliases": [], "archived": True}
    )
    await fixture.connection.execute(
        f"""INSERT INTO "{fixture.catalog}".registry_fhir_catalog_network
            VALUES($1,8,'Z Shadowed','[]',false,1,ARRAY['medical'],'z shadowed'),
                  ($1,9,'PPO% Native','[]',false,1,ARRAY['medical'],'ppo% native'),
                  ($1,10,'PPO% Shadowed','[]',false,1,ARRAY['medical'],'ppo% shadowed')""",
        GENERATION,
    )
    policy = RegistryOrganizationVisibility()
    scenarios = (
        (catalog.RegistryNetworkCatalogQuery(search="PPO%", limit=1), (), 2, (8,)),
        (catalog.RegistryNetworkCatalogQuery(search="PPO%", limit=1, offset=1), (), 2, (9,)),
        (catalog.RegistryNetworkCatalogQuery(search="PPO%", limit=1, offset=2), (), 2, ()),
        (catalog.RegistryNetworkCatalogQuery(search="PPO%"), (8,), 1, (9,)),
        (catalog.RegistryNetworkCatalogQuery(search="PPO%", archived=True), (), 1, (10,)),
        (catalog.RegistryNetworkCatalogQuery(search="PPO_"), (), 0, ()),
        (catalog.RegistryNetworkCatalogQuery(search="Shadowed"), (), 0, ()),
        (catalog.RegistryNetworkCatalogQuery(), (), 3, (8, 7, 9)),
    )

    async def unscoped(*_args):
        return None

    for query, exclusions, total, expected_ids in scenarios:
        inputs = (fixture.connection, fixture.manifest, query, exclusions, fixture.control)
        actual = await catalog._page(*inputs, organization_policy=policy)
        with monkeypatch.context() as patch:
            patch.setattr(catalog, "_list_page_keys", unscoped)
            assert actual == await catalog._page(*inputs, organization_policy=policy)
        assert actual["total"] == total
        assert tuple(network["network_id"] for network in actual["items"]) == expected_ids


async def test_manual_name_archive_and_binding_override_remain_authoritative(effective_catalog_db, monkeypatch):
    db = effective_catalog_db
    await manual_record(
        db, "network", 7, {"network_id": 7, "display_name": "Manual name", "aliases": [], "archived": True}
    )
    await manual_record(
        db, "network", 8, {"network_id": 8, "display_name": "Remapped network", "aliases": [], "archived": False}
    )
    await manual_record(
        db,
        "network_binding",
        uuid4(),
        {
            "network_id": 8,
            "source_system": "fhir",
            "source_id": "source-alias",
            "source_key": "network-one",
            "archived": False,
        },
    )

    def unexpected_full_check(_ctes):
        pytest.fail("Unambiguous manual overrides should avoid scanning imported bindings for conflicts")

    monkeypatch.setattr(effective, "_catalog_conflict_sql", unexpected_full_check)
    catalog_records = await effective.effective_catalog_sql(db.connection, db.manifest, db.control)
    native_binding = await db.connection.fetchrow(
        "SELECT record_json FROM "
        + catalog_records
        + " effective WHERE record_kind='network_binding' AND record_key='00000000-0000-0000-0000-000000000001'"
    )
    assert json.loads(native_binding["record_json"])["network_id"] == 8
    scoped = await effective.effective_catalog_sql(
        db.connection, db.manifest, db.control, selected_networks="$1::integer[]"
    )
    scoped_lookup = (
        "SELECT record_json FROM "
        + scoped
        + " effective WHERE record_kind='network_binding' AND record_key='00000000-0000-0000-0000-000000000001'"
    )
    assert await db.connection.fetchrow(scoped_lookup, [8]) == native_binding
    assert await db.connection.fetchrow(scoped_lookup, [7]) is None
    count = await _selected_networks(db.connection, (8,), db.manifest, (), db.control, None)
    assert count == (8,)
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await _selected_networks(db.connection, (7,), db.manifest, (), db.control, None)
    await db.connection.execute(
        f"UPDATE \"{db.control}\".registry_approved_record SET record_json=record_json||'{{\"archived\":true}}' WHERE record_kind='network_binding'"
    )
    catalog_records = await effective.effective_catalog_sql(db.connection, db.manifest, db.control)
    assert await db.connection.fetchrow(scoped_lookup, [8]) is None
    assert (
        await db.connection.fetchval(
            "SELECT count(*) FROM "
            + catalog_records
            + " effective WHERE record_kind='network_binding' AND record_json->'archived'='false'::jsonb"
        )
        == 0
    )


async def test_address_visibility_uses_native_catalogue_and_preserves_exclusions(effective_catalog_db):
    db = effective_catalog_db
    catalog_records = await effective.effective_catalog_sql(db.connection, db.manifest, db.control)
    networks = await effective.effective_catalog_networks_sql(db.connection, db.manifest, db.control)
    with effective.effective_catalog_read_scope(catalog_records, networks):
        statement = (
            "SELECT "
            + address_scope._visible_membership_predicate()
            + " FROM (SELECT ARRAY[7] AS canonical_network_ids) address"
        )
        statement = statement.replace(":_network_approved_revision", "$1").replace(":_network_excluded_ids", "$2")
        assert await db.connection.fetchval(statement, 0, []) is True
        assert await db.connection.fetchval(statement, 0, [7]) is False
        scope = SimpleNamespace(manifest=db.manifest, excluded_network_ids=(99,))
        native_clause = select(address_scope._approved_network_clause(literal(7), scope))
        assert (
            await db.connection.fetchval(
                str(native_clause.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
            )
            is True
        )
    assert effective.current_catalog_records_sql() is None


async def test_atomic_pairing_rolls_back_and_retains_source_metadata(effective_catalog_db):
    db = effective_catalog_db
    await db.connection.execute(
        f'INSERT INTO "{db.control}".network_serving_manifest VALUES(10,0,$1::jsonb,$2)',
        json.dumps({"fhir_catalog": GENERATION}),
        "f" * 64,
    )
    with pytest.raises(RuntimeError):
        async with db.connection.transaction():
            await db.connection.execute(f'UPDATE "{db.control}".network_serving_control SET generation_id=10')
            await effective.pair_network_catalog_generation(
                db.connection,
                metadata_generation=GENERATION,
                generation_id=10,
                manifest_sha256="f" * 64,
                approved_revision=0,
                control_schema=db.control,
            )
            raise RuntimeError("publication_aborted")
    assert await db.connection.fetchval(f'SELECT generation_id FROM "{db.control}".network_serving_control') == 9
    assert (
        await db.connection.fetchval(f'SELECT network_generation FROM "{db.control}".registry_fhir_catalog_snapshot')
        == 9
    )
    await db.connection.execute(f'UPDATE "{db.control}".network_serving_control SET generation_id=10')
    await effective.pair_network_catalog_generation(
        db.connection,
        metadata_generation=GENERATION,
        generation_id=10,
        manifest_sha256="f" * 64,
        approved_revision=0,
        control_schema=db.control,
    )
    assert (
        await db.connection.fetchval(f'SELECT network_generation FROM "{db.control}".registry_fhir_catalog_snapshot')
        == 10
    )
    assert (
        await db.connection.fetchval(f'SELECT metadata_generation FROM "{db.control}".network_serving_control')
        == GENERATION
    )
    assert (
        await db.connection.fetchval(
            f'SELECT manifest_sha256 FROM "{db.control}".network_serving_manifest WHERE generation_id=9'
        )
        == MANIFEST
    )


async def test_legacy_manifest_needs_no_native_catalogue(effective_catalog_db):
    db = effective_catalog_db
    assert (
        await effective.effective_catalog_sql(db.connection, SimpleNamespace(source_generations={}), db.control) is None
    )
    assert effective.apply_effective_catalog_sql("SELECT unchanged", None, db.control) == "SELECT unchanged"


async def test_imported_company_links_respect_organization_redaction(effective_catalog_db):
    db = effective_catalog_db
    company_id = str(uuid4())
    await manual_record(
        db, "company", company_id, {"display_name": "Example insurer", "roles": ["insurer"], "archived": False}
    )
    await db.connection.execute(
        f"INSERT INTO \"{db.catalog}\".registry_fhir_catalog_relationship VALUES($1,$2,7,'source-one',$3,$4,$5,NULL,'example insurer','','{{}}')",
        GENERATION,
        "1" * 64,
        "b" * 64,
        UUID(int=1),
        UUID(company_id),
    )
    page = await catalog._page(
        db.connection,
        db.manifest,
        catalog.RegistryNetworkCatalogQuery(),
        (),
        db.control,
        organization_policy=RegistryOrganizationVisibility(),
    )
    assert [company["company_id"] for company in page["items"][0]["companies"]] == [company_id]
    hidden = await catalog._page(
        db.connection,
        db.manifest,
        catalog.RegistryNetworkCatalogQuery(),
        (),
        db.control,
        organization_policy=RegistryOrganizationVisibility(excluded_company_ids=(company_id,)),
    )
    assert hidden["total"] == 1 and hidden["items"][0]["companies"] == []


async def test_conflicting_current_manual_bindings_fail_closed(effective_catalog_db):
    db = effective_catalog_db
    for network_id in (8, 9):
        await manual_record(
            db,
            "network",
            network_id,
            {"network_id": network_id, "display_name": "Manual network", "aliases": [], "archived": False},
        )
        await manual_record(
            db,
            "network_binding",
            uuid4(),
            {
                "network_id": network_id,
                "source_system": "fhir",
                "source_id": "source-alias",
                "source_key": "network-one",
                "archived": False,
            },
        )
    with pytest.raises(ValueError, match="binding_conflict"):
        await effective.effective_catalog_sql(db.connection, db.manifest, db.control)


@pytest.mark.parametrize("source_key", ["network-one", "unmatched"])
async def test_missing_manual_target_refuses_only_a_matching_override(effective_catalog_db, source_key):
    db = effective_catalog_db
    await manual_record(
        db,
        "network_binding",
        uuid4(),
        {
            "network_id": 8,
            "source_system": "fhir",
            "source_id": "source-alias",
            "source_key": source_key,
            "archived": False,
        },
    )
    if source_key == "network-one":
        with pytest.raises(ValueError, match="binding_conflict"):
            await effective.effective_catalog_sql(db.connection, db.manifest, db.control)
    else:
        assert await effective.effective_catalog_sql(db.connection, db.manifest, db.control) is not None


@pytest.mark.parametrize("checksum", [17.0, 18.0])
async def test_aca_conflict_check_matches_legacy_plan_and_checksum(effective_catalog_db, checksum):
    db = effective_catalog_db
    for network_id, key, producer in (
        (8, "ACAPlan/12345CA1234567/17", "registry-native-network-source-v1"),
        (9, "12345CA1234567", "retained-aca"),
    ):
        await manual_record(db, "network", network_id, {"network_id": network_id, "archived": False})
        await manual_record(
            db,
            "network_binding",
            uuid4(),
            {
                "network_id": network_id,
                "source_system": "aca",
                "source_key": key,
                "producer_id": producer,
                "source_scope_json": {"checksum_network": checksum},
                "archived": False,
            },
        )
    check = effective._manual_bindings_may_conflict_sql(f'"{db.control}"', db.manifest.approved_custom_revision)
    assert await db.connection.fetchval(check) is (checksum == 17.0)


async def test_pairing_rejects_changed_manifest_and_rolls_back_head_mismatch(effective_catalog_db):
    db = effective_catalog_db
    with pytest.raises(ValueError, match="manifest_changed"):
        await effective.pair_network_catalog_generation(
            db.connection,
            metadata_generation=GENERATION,
            generation_id=9,
            manifest_sha256="f" * 64,
            approved_revision=0,
            control_schema=db.control,
        )
    await db.connection.execute(
        f'INSERT INTO "{db.control}".network_serving_manifest VALUES(10,0,$1::jsonb,$2)',
        json.dumps({"fhir_catalog": GENERATION}),
        "f" * 64,
    )
    with pytest.raises(ValueError, match="head_changed"):
        async with db.connection.transaction():
            await effective.pair_network_catalog_generation(
                db.connection,
                metadata_generation=GENERATION,
                generation_id=10,
                manifest_sha256="f" * 64,
                approved_revision=0,
                control_schema=db.control,
            )
    assert (
        await db.connection.fetchval(f'SELECT network_generation FROM "{db.control}".registry_fhir_catalog_snapshot')
        == 9
    )


async def test_native_policy_probes_existing_network_key_without_catalogue_scan(effective_catalog_db):
    db = effective_catalog_db
    await db.connection.execute(
        f'''INSERT INTO "{db.catalog}".registry_fhir_catalog_network
      SELECT $1,sequence,'network-'||sequence,'[]',false,1,NULL,'network-'||sequence
      FROM generate_series(100,10099) sequence''',
        GENERATION,
    )
    catalog_records = await effective.effective_catalog_sql(db.connection, db.manifest, db.control)
    networks = await effective.effective_catalog_networks_sql(db.connection, db.manifest, db.control)
    statements = []
    for selected in (None, networks):
        with effective.effective_catalog_read_scope(catalog_records, selected):
            predicate = (
                address_scope._visible_membership_predicate()
                .replace(":_network_approved_revision", "$1")
                .replace(":_network_excluded_ids", "$2")
            )
        statements.append(
            "SELECT "
            + predicate
            + " FROM (SELECT ARRAY[sequence] AS canonical_network_ids FROM generate_series(10080,10099) sequence) addresses"
        )
    assert await db.connection.fetch(statements[0], 0, []) == await db.connection.fetch(statements[1], 0, [])
    plan = json.loads(await db.connection.fetchval("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) " + statements[1], 0, []))[0][
        "Plan"
    ]
    probes = [node for node in _plan_nodes(plan) if node.get("Relation Name") == "registry_fhir_catalog_network"]
    assert len(probes) == 1 and probes[0]["Node Type"] == "Index Scan"
    assert (
        "metadata_generation" in probes[0]["Index Cond"]
        and "network_id = policy_ids.network_id" in probes[0]["Index Cond"]
    )
    assert probes[0]["Actual Loops"] == 20 and probes[0]["Actual Rows"] == 1
