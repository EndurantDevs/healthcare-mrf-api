# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved catalog reads with native publication, scopes and reader isolation."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from process import registry_network_catalog_read as catalog
from process import registry_serving_organization_read as organizations
from process.company_registry_assertion_values import CONTRACT
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.network_source_binding_store import (
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
)
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_management_permissions import (
    install_registry_management_permissions,
    verify_registry_management_permissions,
)
from process.registry_network_catalog_read import (
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacyBatchSelector,
    RegistryNetworkCatalogLegacySelector,
    RegistryNetworkCatalogQuery,
    read_registry_network_catalog,
    read_registry_network_catalog_detail,
    resolve_registry_network_catalog_legacy,
)
from process.registry_serving_organization_read import (
    RegistryOrganizationCatalogQuery,
    read_registry_serving_organizations,
)
from tests.test_company_registry_assertions_postgres import _declared_group, _identifier, _role
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command, _CountedConnection, _create
from tests.test_registry_candidate_composition_postgres import (
    _remove_candidates,
    _roles,
)
from tests.test_registry_company_links_postgres import _links
from tests.test_registry_network_catalog_read import batch_boundary as batch_boundary

pytestmark = pytest.mark.asyncio
ACA = RegistryNetworkSourceCoordinates(
    "aca", "catalog-source", "catalog_dataset", "dataset-one", "producer-one", "edition-one"
)
PTG = replace(ACA, source_system="ptg")
ACA_SCOPE = {
    "issuer_id": "12345",
    "plan_id": "12345CA1234567",
    "plan_year": 2026,
    "state": "CA",
    "checksum_network": -17,
}


def _binding(network_id, coordinates, source_key, scope):
    return {
        "binding_id": str(uuid4()),
        **dict(
            zip(
                ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
                coordinates.sql_parameters,
                strict=True,
            )
        ),
        "source_key": source_key,
        "source_scope_json": scope,
        "network_id": network_id,
        "evidence_id": "synthetic-reviewed-scope",
        "evidence_sha256": "b" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }


async def _bind(fixture, rows):
    command = NetworkSourceBindingBatchCommand(
        json.dumps(rows, sort_keys=True).encode(), "Explicit reviewed scopes", uuid4().hex
    )
    async with fixture.connection.transaction():
        return (
            await apply_network_source_binding_batch(
                fixture.connection, command, fixture.seed.actor, control_schema=fixture.control_schema
            )
        )["records"]


async def _publish(fixture, approved_revision, expected_head):
    request_id = uuid4()
    # Register cleanup before the native composer can create any candidate.
    fixture.requests.append(request_id)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        pin = await pin_approved_membership_source(
            fixture.connection, approved_revision=approved_revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + pin.generation_id))
    target, source = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=approved_revision,
        expected_head=expected_head,
        address_sources=RegistryCompositionAddressSources(fixture.base),
        writer_roles=_roles(fixture),
        control_schema=fixture.control_schema,
    )
    fixture.targets.append(target)
    await prepare_and_publish_network_candidate(
        fixture.connection, target, source, **_roles(fixture), control_schema=fixture.control_schema
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    assert manifest.approved_custom_revision == approved_revision
    assert manifest.source_generations["custom_membership"] == pin.generation_id
    return manifest


async def _catalog_records(fixture):
    """Create the exact named network and organization fixture records."""
    seed = await _seed(fixture)
    fixture.seed = seed
    alpha = await _draft(
        fixture,
        replace(
            _create("network"),
            record_id=seed.network["record_id"],
            allocation_key=None,
            operation="correct",
            expected_revision=1,
            fields={"display_name": "Alpha Network", "aliases": ["First Alias"]},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    beta = await _draft(
        fixture, replace(_create("network"), fields={"display_name": "Beta Network", "aliases": []}), seed.actor
    )
    gamma = await _draft(
        fixture, replace(_create("network"), fields={"display_name": "Gamma Network", "aliases": []}), seed.actor
    )
    archive = await _draft(
        fixture, replace(_create("network"), fields={"display_name": "Zeta Network", "aliases": []}), seed.actor
    )
    archive = await _draft(
        fixture,
        replace(
            _create("network"),
            record_id=archive["record_id"],
            allocation_key=None,
            operation="archive",
            expected_revision=1,
            fields={},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    company = await _draft(fixture, _create("company"), seed.actor)
    group = await _draft(fixture, _create("group"), seed.actor)
    return SimpleNamespace(alpha=alpha, beta=beta, gamma=gamma, archive=archive, company=company, group=group)


async def _catalog_approval(fixture):
    """Approve complete source bindings and company relationships together."""
    links = await _draft(
        fixture,
        _links(fixture.records.company, (fixture.records.alpha, fixture.records.beta), fixture.records.group),
        fixture.seed.actor,
    )
    bindings = await _bind(
        fixture,
        [
            _binding(fixture.records.alpha["record_id"], ACA, "alpha-scope", ACA_SCOPE),
            _binding(fixture.records.beta["record_id"], ACA, "beta-scope", {**ACA_SCOPE, "checksum_network": 42}),
            _binding(
                fixture.records.gamma["record_id"],
                PTG,
                "gamma-scope",
                {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
            ),
        ],
    )
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(
            fixture.connection,
            fixture.control_schema,
            fixture.records.alpha,
            fixture.records.beta,
            fixture.records.gamma,
            fixture.records.archive,
            fixture.records.company,
            fixture.records.group,
            links,
            *bindings,
        ),
        fixture.seed.actor,
    )
    return approval


async def _catalog_permissions(fixture):
    """Enroll only the precise protected owner and verify reader restrictions."""
    # Use the existing precise registry ACL installer, with the existing API role.
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'ALTER SCHEMA "{fixture.control_schema}" OWNER TO "{fixture.roles["owner"]}"')
        fixture.permissions = await install_registry_management_permissions(
            fixture.connection,
            api_role=fixture.roles["reader"],
            owner_role=fixture.roles["owner"],
            control_schema=fixture.control_schema,
        )
    # Native schema transfer requires the protected owner enrollment used by composition fixtures.
    database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    assert await fixture.connection.fetchval(
        "SELECT has_database_privilege($1,current_database(),'CREATE')", fixture.roles["owner"]
    )
    for kind in ("reader", "outsider"):
        assert not await fixture.connection.fetchval(
            "SELECT has_database_privilege($1,current_database(),'CREATE')", fixture.roles[kind]
        )


@pytest.fixture
async def catalog_db(custom_db):
    """Own candidate and reader cleanup from before their first creation."""
    fixture = custom_db
    fixture.requests, fixture.targets, fixture.reader = [], [], None
    # The reused fixtures already register schema/role teardown before their DDL.
    # This finally is established before the additional connection and candidates.
    try:
        assert 180000 <= int(await fixture.connection.fetchval("SHOW server_version_num")) < 190000
        fixture.records = await _catalog_records(fixture)
        approval = await _catalog_approval(fixture)
        await _catalog_permissions(fixture)
        fixture.manifest = await _publish(fixture, approval["approved_revision"], 0)
        fixture.reader = await asyncpg.connect(
            os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
        )
        yield fixture
    finally:
        try:
            if fixture.reader is not None:
                await fixture.reader.close()
                assert fixture.reader.is_closed()
        finally:
            try:
                for request_id in reversed(fixture.requests):
                    await _remove_candidates(fixture, fixture.targets, request_id)
            finally:
                # Protected ownership was installed for this fixture schema;
                # drop it before original role teardown encounters other-owned
                # recipe tables. The outer schema drop is idempotent.
                await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{fixture.control_schema}" CASCADE')
                assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", fixture.control_schema) is None
        # Outer genuine fixtures subsequently remove composition/source/control
        # schemas and exact three roles, verifying absence and closing sessions.


@asynccontextmanager
async def _reader(fixture, *, isolation="repeatable_read", readonly=True, role="reader"):
    async with fixture.reader.transaction(isolation=isolation, readonly=readonly):
        await fixture.reader.execute(f'SET LOCAL ROLE "{fixture.roles[role]}"')
        yield fixture.reader


async def _page(fixture, query=None, exclusions=()):
    async with _reader(fixture) as connection:
        return await read_registry_network_catalog(
            connection,
            query or RegistryNetworkCatalogQuery(),
            excluded_network_ids=exclusions,
            control_schema=fixture.control_schema,
        )


async def test_native_counts_details_archives_and_unknown_capabilities(catalog_db):
    fixture = catalog_db
    page = await _page(fixture)
    assert page["total"] == 3 and [row["display_name"] for row in page["items"]] == [
        "Alpha Network",
        "Beta Network",
        "Gamma Network",
    ]
    assert page["generation"] == str(fixture.manifest.generation_id)
    assert page["approved_custom_revision"] == str(fixture.manifest.approved_custom_revision)
    assert [row["directory_available"] for row in page["items"]] == [True, False, False]
    assert all(row["priceable"] is None and row["benefit_codes"] is None for row in page["items"])
    async with _reader(fixture) as connection:
        detail = await read_registry_network_catalog_detail(
            connection,
            fixture.records.alpha["record_id"],
            excluded_network_ids=(),
            control_schema=fixture.control_schema,
        )
        assert detail["items"] == page["items"][:1]
        assert detail["items"][0]["companies"][0]["company_id"] == fixture.records.company["record_id"]
        assert detail["items"][0]["companies"][0]["group_id"] == fixture.records.group["record_id"]
        assert detail["items"][0]["source_bindings"][0]["source_scope"] == ACA_SCOPE
    assert (await _page(fixture, RegistryNetworkCatalogQuery(archived=None)))["total"] == 4
    archived = await _page(fixture, RegistryNetworkCatalogQuery(archived=True))
    assert archived["total"] == 1 and archived["items"][0]["network_id"] == fixture.records.archive["record_id"]
    assert (await _page(fixture, RegistryNetworkCatalogQuery(search="first alias")))["total"] == 1


async def test_native_exclusions_precede_counts_offsets_and_details(catalog_db):
    fixture = catalog_db
    excluded = (fixture.records.alpha["record_id"],)
    first = await _page(fixture, RegistryNetworkCatalogQuery(limit=1), excluded)
    second = await _page(fixture, RegistryNetworkCatalogQuery(limit=1, offset=1), excluded)
    end = await _page(fixture, RegistryNetworkCatalogQuery(limit=1, offset=2), excluded)
    assert first["total"] == second["total"] == end["total"] == 2
    assert first["items"][0]["network_id"] == fixture.records.beta["record_id"]
    assert second["items"][0]["network_id"] == fixture.records.gamma["record_id"]
    assert end["items"] == ()
    async with _reader(fixture) as connection:
        with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
            await read_registry_network_catalog_detail(
                connection, excluded[0], excluded_network_ids=excluded, control_schema=fixture.control_schema
            )
        with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
            await read_registry_network_catalog_detail(
                connection, 2147483647, excluded_network_ids=(), control_schema=fixture.control_schema
            )


async def test_native_exact_six_coordinates_and_unique_scoped_legacy(catalog_db):
    fixture = catalog_db
    exact = await _page(fixture, RegistryNetworkCatalogQuery(source=ACA))
    assert exact["total"] == 2 and [row["network_id"] for row in exact["items"]] == [
        fixture.records.alpha["record_id"],
        fixture.records.beta["record_id"],
    ]
    for field in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"):
        changed = replace(ACA, **{field: "fhir" if field == "source_system" else "different"})
        assert (await _page(fixture, RegistryNetworkCatalogQuery(source=changed)))["total"] == 0
    selector = RegistryNetworkCatalogLegacySelector(ACA, "checksum_network", "-17", json.dumps(ACA_SCOPE).encode())
    async with _reader(fixture) as connection:
        resolved = await resolve_registry_network_catalog_legacy(
            connection, selector, excluded_network_ids=(), control_schema=fixture.control_schema
        )
        assert resolved["items"][0]["network_id"] == fixture.records.alpha["record_id"]
        with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
            await resolve_registry_network_catalog_legacy(
                connection,
                replace(selector, source=replace(ACA, edition_id="different")),
                excluded_network_ids=(),
                control_schema=fixture.control_schema,
            )


async def test_native_global_ambiguity_survives_client_exclusion(catalog_db):
    fixture = catalog_db
    added = await _bind(fixture, [_binding(fixture.records.beta["record_id"], ACA, "reviewed-second-alias", ACA_SCOPE)])
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *added),
        fixture.seed.actor,
    )
    await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
    selector = RegistryNetworkCatalogLegacySelector(ACA, "checksum_network", "-17", json.dumps(ACA_SCOPE).encode())
    for exclusions in ((), (fixture.records.beta["record_id"],)):
        async with _reader(fixture) as connection:
            with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
                await resolve_registry_network_catalog_legacy(
                    connection, selector, excluded_network_ids=exclusions, control_schema=fixture.control_schema
                )


async def test_native_retained_generation_after_approval_and_serving_head_advance(catalog_db):
    fixture = catalog_db
    async with _reader(fixture) as connection:
        retained_page_by_field = await read_registry_network_catalog(
            connection, RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema=fixture.control_schema
        )
        changed = await _draft(
            fixture,
            replace(
                _create("network"),
                record_id=fixture.records.alpha["record_id"],
                allocation_key=None,
                operation="correct",
                expected_revision=2,
                fields={"display_name": "Delta Network", "aliases": ["First Alias"]},
                idempotency_key=uuid4().hex,
            ),
            fixture.seed.actor,
        )
        approval = await _approve(
            fixture.connection,
            fixture.control_schema,
            await _command(fixture.connection, fixture.control_schema, changed),
            fixture.seed.actor,
        )
        # Advancing draft/approval heads cannot replace an in-flight read pin.
        assert (
            await read_registry_network_catalog(
                connection,
                RegistryNetworkCatalogQuery(),
                excluded_network_ids=(),
                control_schema=fixture.control_schema,
            )
            == retained_page_by_field
        )
    after_approval = await _page(fixture)
    assert after_approval == retained_page_by_field
    new_manifest = await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
    current_page_by_field = await _page(fixture)
    assert current_page_by_field["generation"] == str(new_manifest.generation_id)
    assert [network_by_field["display_name"] for network_by_field in current_page_by_field["items"]] == [
        "Beta Network",
        "Delta Network",
        "Gamma Network",
    ]
    retained = await _page(fixture, RegistryNetworkCatalogQuery(generation_id=fixture.manifest.generation_id))
    assert retained == retained_page_by_field


@pytest.mark.parametrize("loss", ["approved_map", "directory", "reader_acl"])
async def test_native_proof_loss_refuses_instead_of_mutable_fallback(catalog_db, loss):
    fixture = catalog_db
    if loss == "approved_map":
        await fixture.connection.execute(
            f"DELETE FROM \"{fixture.control_schema}\".registry_approved_record WHERE approved_revision=$1 AND record_kind='membership'",
            fixture.manifest.approved_custom_revision,
        )
    elif loss == "directory":
        await fixture.connection.execute(f'DELETE FROM "{fixture.manifest.schema_name}".network_membership')
    else:
        await fixture.connection.execute(
            f'REVOKE SELECT ON "{fixture.control_schema}".registry_approved_record FROM "{fixture.roles["reader"]}"'
        )
    with pytest.raises(RegistryNetworkCatalogError):
        await _page(fixture)


async def test_native_reader_permissions_are_precise_and_outsider_refuses(catalog_db):
    fixture = catalog_db
    async with fixture.connection.transaction():
        assert (
            await verify_registry_management_permissions(
                fixture.connection,
                api_role=fixture.roles["reader"],
                owner_role=fixture.roles["owner"],
                control_schema=fixture.control_schema,
            )
            == fixture.permissions
        )
    async with _reader(fixture, readonly=False) as connection:
        assert not await connection.fetchval(
            "SELECT has_table_privilege(current_user,$1,'INSERT')",
            f'"{fixture.control_schema}".registry_approved_record',
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(
                    f'INSERT INTO "{fixture.control_schema}".registry_approved_record SELECT * FROM "{fixture.control_schema}".registry_approved_record LIMIT 0'
                )
    async with _reader(fixture, role="outsider") as connection:
        with pytest.raises(RegistryNetworkCatalogError, match="unavailable"):
            await read_registry_network_catalog(
                connection,
                RegistryNetworkCatalogQuery(),
                excluded_network_ids=(),
                control_schema=fixture.control_schema,
            )


@pytest.mark.parametrize("isolation,readonly", [("read_committed", True), ("repeatable_read", False)])
async def test_native_unpinned_transaction_refuses(catalog_db, isolation, readonly):
    async with _reader(catalog_db, isolation=isolation, readonly=readonly) as connection:
        with pytest.raises(RegistryNetworkCatalogError, match="transaction_required"):
            await read_registry_network_catalog(
                connection,
                RegistryNetworkCatalogQuery(),
                excluded_network_ids=(),
                control_schema=catalog_db.control_schema,
            )


async def test_native_cancellation_rolls_back_reader_and_releases_lock(catalog_db):
    fixture = catalog_db
    task = None
    blocker = fixture.connection.transaction()
    await blocker.start()
    try:
        await fixture.connection.execute(
            f'LOCK TABLE "{fixture.control_schema}".registry_approved_record IN ACCESS EXCLUSIVE MODE NOWAIT'
        )

        async def blocked_read():
            return await _page(fixture)

        task = asyncio.create_task(blocked_read())
        pid = fixture.reader.get_server_pid()
        async with asyncio.timeout(5):
            while not await fixture.connection.fetchval(
                "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1", pid
            ):
                await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, 5)
            except asyncio.CancelledError:
                assert task.cancelled()
        await blocker.rollback()
    assert not fixture.reader.is_in_transaction()
    assert await fixture.reader.fetchval("SELECT 1") == 1
    assert (await _page(fixture))["total"] == 3


async def test_native_approved_multibyte_names_aliases_and_search(catalog_db):
    fixture = catalog_db
    edits = []
    for kind, original, fields in (
        ("network", fixture.records.alpha, {"display_name": "é" * 512, "aliases": ["漢" * 512]}),
        (
            "company",
            fixture.records.company,
            {"display_name": "界" * 512, "aliases": [], "roles": fixture.records.company["record"]["roles"]},
        ),
    ):
        command = replace(
            _create(kind),
            record_id=original["record_id"] if kind == "network" else UUID(original["record_id"]),
            allocation_key=None,
            operation="correct",
            expected_revision=original["revision"],
            fields=fields,
            idempotency_key=uuid4().hex,
        )
        edits.append(await _draft(fixture, command, fixture.seed.actor))
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *edits),
        fixture.seed.actor,
    )
    await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
    page = await _page(fixture, RegistryNetworkCatalogQuery(search="é" * 512))
    assert page["total"] == 1
    network = page["items"][0]
    assert network["display_name"] == "é" * 512 and network["aliases"] == ["漢" * 512]
    assert network["companies"][0]["display_name"] == "界" * 512


async def _declared_catalog_links(fixture):
    """Draft genuine explicit assertions with retained, deliberately varied periods."""
    from tests.test_registry_company_links_postgres import _assertion

    catalog_records = fixture.records
    assertions = [
        _assertion(
            catalog_records.company,
            catalog_records.alpha,
            benefit_domain="dental",
            valid_from="2020-01-01",
            valid_to="2021-01-01",
        ),
        _assertion(
            catalog_records.company,
            catalog_records.beta,
            benefit_domain="vision",
            valid_from="2030-01-01",
            valid_to=None,
        ),
        _assertion(
            catalog_records.company, catalog_records.beta, benefit_domain="dental", valid_from=None, valid_to=None
        ),
    ]
    return await _draft(
        fixture,
        replace(
            _links(catalog_records.company, (catalog_records.alpha, catalog_records.beta), catalog_records.group),
            operation="correct",
            expected_revision=1,
            fields={
                "network_ids": [catalog_records.alpha["record_id"], catalog_records.beta["record_id"]],
                "group_id": catalog_records.group["record_id"],
                "network_assertions": assertions,
            },
        ),
        fixture.seed.actor,
    )


async def test_native_declared_domains_use_only_explicit_approved_pinned_links_before_paging(catalog_db):
    """Filter approved declarations before paging without temporal or pricing inference."""
    fixture = catalog_db
    catalog_records = fixture.records
    links = await _declared_catalog_links(fixture)
    assert (await _page(fixture, RegistryNetworkCatalogQuery(benefit_code="dental")))["total"] == 0
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, links),
        fixture.seed.actor,
    )
    latest = await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
    page = await _page(fixture, RegistryNetworkCatalogQuery(benefit_code="dental", limit=1, offset=1))
    assert page["generation"] == str(latest.generation_id) and page["total"] == 2
    assert page["items"][0]["network_id"] == catalog_records.beta["record_id"]
    assert page["items"][0]["benefit_codes"] == ["dental", "vision"]
    assert page["items"][0]["priceable"] is None and not page["items"][0]["directory_available"]
    assert (
        await _page(fixture, RegistryNetworkCatalogQuery(benefit_code="dental"), (catalog_records.alpha["record_id"],))
    )["total"] == 1
    assert (await _page(fixture, RegistryNetworkCatalogQuery(benefit_code="dental", source=PTG)))["total"] == 0
    old = await _page(
        fixture, RegistryNetworkCatalogQuery(generation_id=fixture.manifest.generation_id, benefit_code="dental")
    )
    assert old["total"] == 0
    unfiltered = await _page(fixture)
    assert unfiltered["items"][2]["network_id"] == catalog_records.gamma["record_id"]
    assert unfiltered["items"][2]["benefit_codes"] is None
    assert links["record"]["network_assertions"][0]["valid_to"] == "2021-01-01"


async def test_v2_company_exclusion_precedes_declared_benefits_and_group_disclosure(catalog_db):
    fixture = catalog_db
    links = await _declared_catalog_links(fixture)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, links),
        fixture.seed.actor,
    )
    latest = await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
    company_id, group_id = fixture.records.company["record_id"], fixture.records.group["record_id"]
    async with _reader(fixture) as connection:
        default = await read_registry_network_catalog(
            connection,
            RegistryNetworkCatalogQuery(),
            excluded_network_ids=(),
            organization_policy=RegistryOrganizationVisibility(),
            control_schema=fixture.control_schema,
        )
        hidden = await read_registry_network_catalog(
            connection,
            RegistryNetworkCatalogQuery(),
            excluded_network_ids=(),
            organization_policy=RegistryOrganizationVisibility((company_id,), ()),
            control_schema=fixture.control_schema,
        )
        assert hidden["total"] == default["total"] and hidden["generation"] == str(latest.generation_id)
        assert all(
            not network_by_field["companies"] and network_by_field["benefit_codes"] is None
            for network_by_field in hidden["items"]
        )
        filtered = await read_registry_network_catalog(
            connection,
            RegistryNetworkCatalogQuery(benefit_code="dental", limit=1),
            excluded_network_ids=(),
            organization_policy=RegistryOrganizationVisibility((company_id,), ()),
            control_schema=fixture.control_schema,
        )
        assert filtered["total"] == 0 and filtered["items"] == ()
        grouped = await read_registry_network_catalog_detail(
            connection,
            fixture.records.alpha["record_id"],
            excluded_network_ids=(),
            organization_policy=RegistryOrganizationVisibility((), (group_id,)),
            control_schema=fixture.control_schema,
        )
        assert grouped["items"][0]["companies"][0]["company_id"] == company_id
        assert grouped["items"][0]["companies"][0]["group_id"] is None
        assert grouped["items"][0]["benefit_codes"] == ["dental"]


async def _organization_read(fixture, kind, record_ids, **options):
    async with _reader(fixture) as connection:
        return await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery(
                kind,
                record_ids=record_ids,
                **{key: entry for key, entry in options.items() if key != "organization_policy"},
            ),
            control_schema=fixture.control_schema,
            **({"organization_policy": options["organization_policy"]} if "organization_policy" in options else {}),
        )


async def _organization_approve_publish(fixture, *records):
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *records),
        fixture.seed.actor,
    )
    return await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)


@pytest.mark.parametrize("kind", ["company", "group"])
async def test_organization_names_and_archives_change_only_with_serving_publication(catalog_db, kind):
    fixture = catalog_db
    original = getattr(fixture.records, kind)
    identities = (original["record_id"],)
    before = await _organization_read(fixture, kind, identities)
    fields_by_name = _create(kind).fields | {"display_name": "Approved replacement", "aliases": ["Approved alias"]}
    correction = await _draft(
        fixture,
        replace(
            _create(kind),
            record_id=UUID(original["record_id"]),
            operation="correct",
            expected_revision=1,
            fields=fields_by_name,
        ),
        fixture.seed.actor,
    )
    assert await _organization_read(fixture, kind, identities) == before
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, correction),
        fixture.seed.actor,
    )
    assert await _organization_read(fixture, kind, identities) == before
    async with _reader(fixture) as connection:
        retained = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery(kind, record_ids=identities),
            control_schema=fixture.control_schema,
        )
        latest = await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)
        assert (
            await read_registry_serving_organizations(
                connection,
                RegistryOrganizationCatalogQuery(kind, record_ids=identities),
                control_schema=fixture.control_schema,
            )
            == retained
        )
    after = await _organization_read(fixture, kind, identities)
    assert after["generation"] == str(latest.generation_id)
    assert after["items"][0]["display_name"] == "Approved replacement"
    assert await _organization_read(fixture, kind, identities, generation_id=fixture.manifest.generation_id) == before
    fixture.manifest = latest
    archived = await _draft(
        fixture,
        replace(
            _create(kind), record_id=UUID(original["record_id"]), operation="archive", expected_revision=2, fields={}
        ),
        fixture.seed.actor,
    )
    assert await _organization_read(fixture, kind, identities) == after
    await _organization_approve_publish(fixture, archived)
    page = await _organization_read(fixture, kind, identities)
    assert page["total"] == 0 and page["items"] == []


async def test_organization_record_visibility_precedes_total_and_offset(catalog_db):
    fixture = catalog_db
    companies = [await _draft(fixture, _create("company"), fixture.seed.actor) for _ in range(3)]
    await _organization_approve_publish(fixture, *companies)
    allowed_ids = tuple(sorted(company["record_id"] for company in companies)[1:])
    page = await _organization_read(fixture, "company", allowed_ids, limit=1, offset=1)
    assert page["total"] == 2 and page["items"][0]["company_id"] == allowed_ids[1]
    denied = await _organization_read(fixture, "company", ())
    assert denied["total"] == 0 and denied["items"] == []


async def test_organization_response_budget_refuses_without_an_unbounded_payload(catalog_db, monkeypatch):
    fixture = catalog_db
    monkeypatch.setattr(organizations, "MAX_RESPONSE_BYTES", 512)
    with pytest.raises(RegistryNetworkCatalogError, match="registry_serving_organization_unavailable"):
        await _organization_read(fixture, "company", (fixture.records.company["record_id"],), limit=1)


async def test_organization_company_evidence_does_not_expand_group_visibility(catalog_db):
    """Filter group targets independently while retaining exact declared company evidence."""
    fixture = catalog_db
    company, group = fixture.records.company, fixture.records.group
    group_command = _create("group")
    private_group = await _draft(
        fixture,
        replace(group_command, fields=group_command.fields | {"group_kind": "naic_group"}),
        fixture.seed.actor,
    )
    groups = [
        _declared_group(
            company["record_id"], group_target["record_id"], group_kind=group_target["record"]["group_kind"]
        )
        for group_target in (group, private_group)
    ]
    role, identifier = _role(), _identifier()
    correction = await _draft(
        fixture,
        replace(
            _create("company"),
            record_id=UUID(company["record_id"]),
            operation="correct",
            expected_revision=1,
            fields={
                "display_name": "Approved company",
                "roles": ["employer"],
                "aliases": ["Approved alias"],
                "assertions": {
                    "contract": CONTRACT,
                    "company_id": company["record_id"],
                    "expected_revision": 1,
                    "role_assertions": [role],
                    "identifier_assertions": [identifier],
                    "group_assertions": groups,
                },
            },
        ),
        fixture.seed.actor,
    )
    await _organization_approve_publish(fixture, private_group, correction)
    page = await _organization_read(
        fixture, "company", (company["record_id"],), allowed_group_ids=(group["record_id"],)
    )
    company_record = page["items"][0]
    assert company_record["role_assertions"] == [role] and company_record["identifier_assertions"] == [identifier]
    assert company_record["group_assertions"] == groups[:1] and company_record["aliases"] == ["Approved alias"]
    assert "correction_predecessors" not in company_record and "created_at" not in company_record
    hidden = await _organization_read(fixture, "company", (company["record_id"],))
    assert hidden["items"][0]["group_assertions"] == []
    group_page = await _organization_read(fixture, "group", (group["record_id"],))
    assert group_page["total"] == 1 and group_page["items"][0]["group_id"] == group["record_id"]
    visible = await _organization_read(fixture, "company", None, organization_policy=RegistryOrganizationVisibility())
    assert sorted(visible["items"][0]["group_assertions"], key=lambda entry: entry["assertion_id"]) == sorted(
        groups, key=lambda entry: entry["assertion_id"]
    )
    policy = RegistryOrganizationVisibility((), (private_group["record_id"],))
    restricted = await _organization_read(fixture, "company", None, organization_policy=policy)
    assert restricted["items"][0]["group_assertions"] == groups[:1]


@pytest.mark.parametrize("damage", ["kind", "none", "duplicate", "identity", "groups", "limit", "offset", "generation"])
async def test_organization_invalid_scope_never_reaches_database(damage):
    identity = str(uuid4())
    query_by_name = dict(record_ids=(identity,))
    kind = "company"
    if damage == "kind":
        kind = "provider"
    if damage == "none":
        query_by_name["record_ids"] = None
    if damage == "duplicate":
        query_by_name["record_ids"] = (identity, identity)
    if damage == "identity":
        query_by_name["record_ids"] = ("unknown",)
    if damage == "groups":
        query_by_name["allowed_group_ids"] = None
    if damage == "limit":
        query_by_name["limit"] = 101
    if damage == "offset":
        query_by_name["offset"] = -1
    if damage == "generation":
        query_by_name["generation_id"] = True
    with pytest.raises(ValueError):
        await read_registry_serving_organizations(object(), RegistryOrganizationCatalogQuery(kind, **query_by_name))


async def test_default_visible_exceptions_filter_before_total_offset_and_exact_detail(catalog_db):
    fixture = catalog_db
    companies = [await _draft(fixture, _create("company"), fixture.seed.actor) for _ in range(3)]
    latest = await _organization_approve_publish(fixture, *companies)
    identities = tuple(
        sorted(
            [fixture.records.company["record_id"], *(company_by_field["record_id"] for company_by_field in companies)]
        )
    )
    policy = RegistryOrganizationVisibility((identities[0],), ())
    async with _reader(fixture) as connection:
        default = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery("company"),
            organization_policy=RegistryOrganizationVisibility(),
            control_schema=fixture.control_schema,
        )
        assert default["total"] == 4 and default["generation"] == str(latest.generation_id)
        retained = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery("company", generation_id=fixture.manifest.generation_id),
            organization_policy=RegistryOrganizationVisibility(),
            control_schema=fixture.control_schema,
        )
        assert retained["total"] == 1 and retained["generation"] == str(fixture.manifest.generation_id)
        page = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery("company", limit=1, offset=1),
            organization_policy=policy,
            control_schema=fixture.control_schema,
        )
        assert page["total"] == 3 and page["items"][0]["company_id"] == identities[2]
        denied = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery("company", record_ids=(identities[0],)),
            organization_policy=policy,
            control_schema=fixture.control_schema,
        )
        assert denied["total"] == 0 and denied["items"] == []
        groups = await read_registry_serving_organizations(
            connection,
            RegistryOrganizationCatalogQuery("group"),
            organization_policy=policy,
            control_schema=fixture.control_schema,
        )
        assert groups["total"] == 1


def _organization_response(kind):
    organization_by_field = {
        kind + "_id": "11111111-1111-4111-8111-111111111111",
        "display_name": "Example Organization",
        "aliases": [],
        "archived": False,
        "revision": 1,
    }
    if kind == "group":
        organization_by_field["group_kind"] = "corporate_parent"
    else:
        organization_by_field.update(
            display_name_origin="custom", roles=[], role_assertions=[], identifier_assertions=[], group_assertions=[]
        )
    return organization_by_field


@pytest.mark.parametrize(
    "kind,poison",
    [
        ("company", "not-list"),
        ("company", "not-record"),
        ("company", "extra-field"),
        ("company", "foreign-id"),
        ("company", "duplicate"),
        ("company", "archived"),
        ("company", "aliases-type"),
        ("company", "aliases-bound"),
        ("company", "assertions-type"),
        ("group", "group-kind"),
        ("group", "accounting"),
    ],
)
async def test_organization_reader_refuses_corrupt_closed_approved_page(batch_boundary, kind, poison):
    connection, _, _ = batch_boundary
    organization_by_field = _organization_response(kind)
    organization_documents = [organization_by_field]
    match poison:
        case "not-list":
            unexpected_by_field = {"items": organization_documents}
            organization_documents = unexpected_by_field
        case "not-record":
            organization_documents = [None]
        case "extra-field":
            organization_by_field["authorized"] = True
        case "foreign-id":
            organization_by_field[kind + "_id"] = "22222222-2222-4222-8222-222222222222"
        case "duplicate":
            organization_documents *= 2
        case "archived":
            organization_by_field["archived"] = True
        case "aliases-type":
            organization_by_field["aliases"] = "Example Alias"
        case "aliases-bound":
            organization_by_field["aliases"] = ["Example Alias"] * 101
        case "assertions-type":
            organization_by_field["group_assertions"] = {}
        case "group-kind":
            organization_by_field["group_kind"] = "unverified"
    connection.page.update(total=2 if poison == "accounting" else 1, rows_json=json.dumps(organization_documents))
    query = RegistryOrganizationCatalogQuery(kind, ("11111111-1111-4111-8111-111111111111",))
    with pytest.raises(RegistryNetworkCatalogError, match="registry_serving_organization_unavailable") as caught:
        await read_registry_serving_organizations(connection, query, control_schema="sample_control")
    assert str(caught.value.__cause__) == "registry_serving_organization_document_invalid"
    assert "registry_approved_record" in connection.calls[-1][0]


@pytest.mark.parametrize(
    "query,policy",
    [
        ({"record_kind": "company", "record_ids": []}, RegistryOrganizationVisibility()),
        (RegistryOrganizationCatalogQuery("company", ()), {"excluded_company_ids": []}),
        (
            RegistryOrganizationCatalogQuery(
                "company", (), allowed_group_ids=("11111111-1111-4111-8111-111111111111",)
            ),
            RegistryOrganizationVisibility(),
        ),
    ],
    ids=["untyped-query", "untyped-policy", "mixed-group-scope"],
)
async def test_organization_scope_refuses_before_pinning_or_querying(batch_boundary, query, policy):
    connection, _, pinned = batch_boundary
    with pytest.raises(ValueError, match="registry_serving_organization_scope_invalid"):
        await read_registry_serving_organizations(
            connection, query, organization_policy=policy, control_schema="sample_control"
        )
    assert connection.calls == [] and pinned == []


@pytest.mark.parametrize("kind", ["company", "group"])
async def test_organization_reader_preserves_valid_closed_pages(batch_boundary, kind):
    connection, _, pinned = batch_boundary
    organization_by_field = _organization_response(kind)
    connection.page.update(total=1, rows_json=json.dumps([organization_by_field]))
    page = await read_registry_serving_organizations(
        connection,
        RegistryOrganizationCatalogQuery(kind, (organization_by_field[kind + "_id"],)),
        control_schema="sample_control",
    )
    assert page["items"] == [organization_by_field] and page["total"] == 1
    assert page["generation"] == "9" and page["approved_custom_revision"] == "4" and pinned == [4]


async def _publish_legacy_batch_bindings(fixture, bindings, *records):
    added = await _bind(fixture, bindings)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *records, *added),
        fixture.seed.actor,
    )
    return await _publish(fixture, approval["approved_revision"], fixture.manifest.generation_id)


async def _legacy_batch_page(fixture, namespace, values, *, exclusions=(), generation=None):
    async with _reader(fixture) as connection:
        return await resolve_registry_network_catalog_legacy(
            connection,
            RegistryNetworkCatalogLegacyBatchSelector(namespace, tuple(values)),
            excluded_network_ids=exclusions,
            generation_id=generation,
            control_schema=fixture.control_schema,
        )


async def test_native_legacy_batch_distinct_identity_and_signed_extrema(catalog_db):
    fixture = catalog_db
    key = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE VALUES(84,$1)',
        key,
    )
    network = await _draft(fixture, replace(_create("network"), allocation_key=key), fixture.seed.actor)
    assert network["record_id"] == 84
    binding_id = await fixture.connection.fetchval(
        f'SELECT binding_id FROM "{fixture.control_schema}".registry_network_binding WHERE source_key=$1', "beta-scope"
    )
    binding_documents = [
        _binding(84, ACA, "beta-scope", {**ACA_SCOPE, "checksum_network": 42})
        | {
            "binding_id": str(binding_id),
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": fixture.records.beta["record_id"],
        }
    ]
    for checksum_value in (-2147483648, 0, 2147483647, 42):
        binding_documents.append(
            _binding(
                84,
                replace(ACA, edition_id="other-edition"),
                "signed-" + str(checksum_value),
                {**ACA_SCOPE, "checksum_network": checksum_value},
            )
        )
    identifier = str(uuid4())
    binding_documents.append(
        _binding(
            84,
            replace(ACA, source_system="fhir"),
            "fhir-alias",
            {"organization_id": "organization-one", "legacy_uuid": identifier, "alias_scope": "explicit-alias"},
        )
    )
    await _publish_legacy_batch_bindings(fixture, binding_documents, network)
    page = await _legacy_batch_page(fixture, "checksum_network", ["42", "0", "-2147483648", "2147483647"])
    assert page["total"] == 1 and [network_document["network_id"] for network_document in page["items"]] == [84]
    assert len(page["items"][0]["source_bindings"]) == 6
    multiple = await _legacy_batch_page(fixture, "checksum_network", ["-17", "42"])
    assert multiple["total"] == 2 and {network_document["network_id"] for network_document in multiple["items"]} == {
        fixture.records.alpha["record_id"],
        84,
    }
    assert (await _legacy_batch_page(fixture, "legacy_fhir_uuid", [identifier]))["items"][0]["network_id"] == 84


async def test_native_legacy_batch_one_and_fifty_share_queries_and_refuse_partial(catalog_db, record_property):
    fixture = catalog_db
    checksum_values = tuple(str(10000 + index) for index in range(50))
    network_documents = [await _draft(fixture, _create("network"), fixture.seed.actor) for _ in checksum_values]
    binding_documents = [
        _binding(
            network_document["record_id"],
            ACA,
            "batch-" + checksum_value,
            {**ACA_SCOPE, "checksum_network": int(checksum_value)},
        )
        for checksum_value, network_document in zip(checksum_values, network_documents, strict=True)
    ]
    current = await _publish_legacy_batch_bindings(fixture, binding_documents, *network_documents)
    counts = []
    for selected in (checksum_values[:1], checksum_values):
        async with _reader(fixture) as connection:
            counted = _CountedConnection(connection)
            page = await resolve_registry_network_catalog_legacy(
                counted,
                RegistryNetworkCatalogLegacyBatchSelector("checksum_network", selected),
                excluded_network_ids=(),
                control_schema=fixture.control_schema,
            )
            counts.append(counted.statements)
            assert page["total"] == len(selected) and {
                network_document["network_id"] for network_document in page["items"]
            } == {network_document["record_id"] for network_document in network_documents[: len(selected)]}
    assert counts[0] == counts[1]
    record_property("legacy_batch_query_counts", json.dumps(counts))
    for selected, exclusions, generation in [
        (checksum_values[:1], (network_documents[0]["record_id"],), None),
        (("-17", "9999"), (), None),
        (checksum_values[:1], (), fixture.manifest.generation_id),
    ]:
        with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
            await _legacy_batch_page(
                fixture, "checksum_network", selected, exclusions=exclusions, generation=generation
            )
    # A draft binding cannot become a source mapping in the retained approved generation.
    await _bind(
        fixture,
        [_binding(fixture.records.beta["record_id"], ACA, "draft-only", {**ACA_SCOPE, "checksum_network": 9999})],
    )
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
        await _legacy_batch_page(fixture, "checksum_network", ["9999"], generation=current.generation_id)
    assert (
        await _legacy_batch_page(fixture, "checksum_network", checksum_values[:1], generation=current.generation_id)
    )["generation"] == str(current.generation_id)


async def test_native_legacy_batch_ambiguity_precedes_hidden_target_filter(catalog_db):
    fixture = catalog_db
    await _publish_legacy_batch_bindings(
        fixture,
        [
            _binding(
                fixture.records.alpha["record_id"],
                replace(ACA, edition_id="another-edition"),
                "ambiguous",
                {**ACA_SCOPE, "checksum_network": 42},
            )
        ],
    )
    for exclusions in ((), (fixture.records.alpha["record_id"],)):
        with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
            await _legacy_batch_page(fixture, "checksum_network", ["42", "-17"], exclusions=exclusions)


async def test_native_legacy_batch_archived_binding_refuses_whole_request(catalog_db):
    fixture = catalog_db
    gamma = fixture.records.gamma
    row = _binding(gamma["record_id"], ACA, "soon-archived", {**ACA_SCOPE, "checksum_network": 999})
    current = await _publish_legacy_batch_bindings(fixture, [row])
    closed = row | {"operation": "close", "expected_revision": 1, "expected_network_id": gamma["record_id"]}
    added = await _bind(fixture, [closed])
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *added),
        fixture.seed.actor,
    )
    await _publish(fixture, approval["approved_revision"], current.generation_id)
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
        await _legacy_batch_page(fixture, "checksum_network", ["-17", "999"])


async def _autocomplete_sources(fixture, monkeypatch, plans, tiers):
    """Create narrowly granted raw-source copies alongside real approved publication."""
    table_name_by_model = {}
    for name in ("Plan", "PlanNetworkTierRaw", "GeoZipLookup", "PlanRatingAreas", "ZipState"):
        model = getattr(catalog, name)
        table = model.__table__.to_metadata(
            MetaData(), schema=fixture.control_schema, name="autocomplete_" + name.lower()
        )
        await fixture.connection.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
        await fixture.connection.execute(
            f'GRANT SELECT ON "{fixture.control_schema}"."{table.name}" TO "{fixture.roles["reader"]}"'
        )
        monkeypatch.setattr(catalog, name, SimpleNamespace(__table__=table))
        table_name_by_model[name] = table.name
    await fixture.connection.copy_records_to_table(
        table_name_by_model["Plan"],
        schema_name=fixture.control_schema,
        columns=("plan_id", "year", "issuer_id", "state", "marketing_name"),
        records=plans,
    )
    await fixture.connection.copy_records_to_table(
        table_name_by_model["PlanNetworkTierRaw"],
        schema_name=fixture.control_schema,
        columns=("plan_id", "year", "issuer_id", "checksum_network", "network_tier"),
        records=tiers,
    )
    return table_name_by_model


async def _autocomplete(fixture, *, query="%", limit=10, exclusions=(), zip_code=None, generation=None, counted=False):
    async with _reader(fixture) as connection:
        selected = _CountedConnection(connection) if counted else connection
        result = await catalog.read_registry_network_autocomplete(
            selected,
            catalog.RegistryNetworkAutocompleteQuery(query, limit, zip_code, generation),
            excluded_network_ids=exclusions,
            control_schema=fixture.control_schema,
        )
        assert await connection.fetchval("SELECT 1") == 1
        return (result, selected.statements) if counted else result


async def _autocomplete_canonical_identity(fixture):
    """Publish checksum42 to canonical84 across two explicit source editions."""
    key = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE VALUES(84,$1)',
        key,
    )
    network = await _draft(fixture, replace(_create("network"), allocation_key=key), fixture.seed.actor)
    assert network["record_id"] == 84
    binding_id = await fixture.connection.fetchval(
        f'SELECT binding_id FROM "{fixture.control_schema}".registry_network_binding WHERE source_key=$1', "beta-scope"
    )
    fixture.manifest = await _publish_legacy_batch_bindings(
        fixture,
        [
            _binding(84, ACA, "beta-scope", {**ACA_SCOPE, "checksum_network": 42})
            | {
                "binding_id": str(binding_id),
                "operation": "rebind",
                "expected_revision": 1,
                "expected_network_id": fixture.records.beta["record_id"],
            },
            _binding(
                84,
                replace(ACA, edition_id="same-target-edition"),
                "same-target-checksum",
                {**ACA_SCOPE, "checksum_network": 42},
            ),
        ],
        network,
    )


async def _autocomplete_hidden_sources(fixture, monkeypatch):
    """Seed sixty hidden and sixty unmapped raw plans before the visible plan."""
    fixture.manifest = await _publish_legacy_batch_bindings(
        fixture,
        [
            _binding(
                84,
                ACA,
                "hidden-raw-" + str(index),
                {**ACA_SCOPE, "plan_id": f"12345CA{index:07d}", "checksum_network": 5000 + index},
            )
            for index in range(60)
        ],
    )
    plans = [(f"12345CA{i:07d}", 2026, 12345, "CA", "Hidden raw") for i in range(120)]
    plans.append((ACA_SCOPE["plan_id"], 2026, 12345, "CA", "Sample é"))
    tiers = [(plan[0], 2026, 12345, 5000 + i, "unmapped") for i, plan in enumerate(plans[:-1])]
    tiers += [(ACA_SCOPE["plan_id"], 2026, 12345, -17, None), (ACA_SCOPE["plan_id"], 2026, 12345, 42, "hidden")]
    await _autocomplete_sources(fixture, monkeypatch, plans, tiers)


async def test_native_autocomplete_authorizes_before_raw_limit_and_globally_rejects_hidden_ambiguity(
    catalog_db, monkeypatch
):
    fixture = catalog_db
    await _autocomplete_canonical_identity(fixture)
    await _autocomplete_hidden_sources(fixture, monkeypatch)
    excluded = (84,)
    response = await _autocomplete(fixture, limit=1, exclusions=excluded)
    assert response["plans"] == [
        {
            "plan_id": ACA_SCOPE["plan_id"],
            "marketing_name": "Sample é",
            "state": "CA",
            "year": 2026,
            "network_checksum": {"-17": None},
        }
    ]
    first, small = await _autocomplete(fixture, limit=1, counted=True)
    larger, large = await _autocomplete(fixture, limit=50, counted=True)
    assert len(first["plans"]) == 1 and len(larger["plans"]) == 50 and small == large == 7
    assert "42" in (await _autocomplete(fixture, query="Sample", exclusions=(42,)))["plans"][0]["network_checksum"]
    retained = fixture.manifest
    fixture.manifest = await _publish_legacy_batch_bindings(
        fixture,
        [
            _binding(
                fixture.records.gamma["record_id"],
                replace(ACA, edition_id="ambiguous-edition"),
                "ambiguous-checksum",
                {**ACA_SCOPE, "checksum_network": 42},
            )
        ],
    )
    current = await _autocomplete(fixture, query="Sample", exclusions=(fixture.records.gamma["record_id"],))
    assert current["plans"][0]["network_checksum"] == {"-17": None}
    old = await _autocomplete(fixture, query="Sample", generation=retained.generation_id)
    assert old["plans"][0]["network_checksum"] == {"-17": None, "42": "hidden"}
    assert old["generation"] == str(retained.generation_id)
    with pytest.raises(RegistryNetworkCatalogError):
        await _autocomplete(fixture, generation=9223372036854775807)


async def _autocomplete_year_sources(fixture, monkeypatch):
    """Publish exact signed checksum scopes and distinct raw year identities."""
    bindings = [
        _binding(
            fixture.records.alpha["record_id"],
            ACA,
            "year-zero",
            {**ACA_SCOPE, "plan_year": 2025, "checksum_network": 0},
        ),
        _binding(fixture.records.alpha["record_id"], ACA, "minimum", {**ACA_SCOPE, "checksum_network": -2147483648}),
        _binding(fixture.records.alpha["record_id"], ACA, "maximum", {**ACA_SCOPE, "checksum_network": 2147483647}),
    ]
    fixture.manifest = await _publish_legacy_batch_bindings(fixture, bindings)
    plan_id = ACA_SCOPE["plan_id"]
    await _autocomplete_sources(
        fixture,
        monkeypatch,
        [
            (plan_id, 2025, 12345, "CA", ""),
            (plan_id, 2026, 12345, "CA", None),
            ("12345NY1234567", 2026, 12345, "NY", "Wrong state"),
        ],
        [
            (plan_id, 2025, 12345, 0, "zero"),
            (plan_id, 2026, 12345, -17, None),
            (plan_id, 2026, 12345, 42, "forty-two"),
            (plan_id, 2026, 12345, -2147483648, "min"),
            (plan_id, 2026, 12345, 2147483647, "max"),
            (plan_id, 2026, 99999, 700, "wrong issuer"),
            ("12345NY1234567", 2026, 12345, -17, "wrong state"),
        ],
    )
    return plan_id


async def test_native_autocomplete_full_plan_identity_signed_checksums_and_bounds(catalog_db, monkeypatch):
    fixture = catalog_db
    plan_id = await _autocomplete_year_sources(fixture, monkeypatch)
    response = await _autocomplete(fixture)
    assert [plan["year"] for plan in response["plans"]] == [2025, 2026]
    assert response["plans"][0]["network_checksum"] == {"0": "zero"}
    assert response["plans"][0]["marketing_name"] == "" and response["plans"][1]["marketing_name"] is None
    assert response["plans"][1]["network_checksum"] == {
        "-2147483648": "min",
        "-17": None,
        "42": "forty-two",
        "2147483647": "max",
    }
    # A real draft binding is readable to management but cannot authorize this serving read.
    await _bind(
        fixture,
        [_binding(fixture.records.alpha["record_id"], ACA, "draft-only", {**ACA_SCOPE, "checksum_network": 700})],
    )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}"."autocomplete_plannetworktierraw" SET issuer_id=12345 WHERE checksum_network=700'
    )
    assert "700" not in (await _autocomplete(fixture))["plans"][1]["network_checksum"]
    # Changing each raw identity coordinate cannot borrow an approved binding for another identity.
    for column, changed, restored in (("state", "NY", "CA"), ("issuer_id", 99999, 12345)):
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}"."autocomplete_plan" SET "{column}"=$1 WHERE year=2026 AND plan_id=$2',
            changed,
            plan_id,
        )
        assert [plan["year"] for plan in (await _autocomplete(fixture))["plans"]] == [2025]
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}"."autocomplete_plan" SET "{column}"=$1 WHERE year=2026 AND plan_id=$2',
            restored,
            plan_id,
        )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}"."autocomplete_plan" SET marketing_name=$1 WHERE year=2026',
        "x" * (catalog.MAX_RESPONSE_BYTES + 1),
    )
    with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
        await _autocomplete(fixture)


async def test_native_autocomplete_zip_replacement_precedence_and_missing_table_savepoints(catalog_db, monkeypatch):
    fixture = catalog_db
    second_id = "12345NY1234567"
    fixture.manifest = await _publish_legacy_batch_bindings(
        fixture,
        [
            _binding(
                fixture.records.beta["record_id"],
                ACA,
                "ny-plan",
                {**ACA_SCOPE, "plan_id": second_id, "state": "NY", "checksum_network": 7},
            )
        ],
    )
    table_name_by_model = await _autocomplete_sources(
        fixture,
        monkeypatch,
        [(ACA_SCOPE["plan_id"], 2026, 12345, "CA", "CA"), (second_id, 2026, 12345, "NY", "NY")],
        [(ACA_SCOPE["plan_id"], 2026, 12345, -17, "ca"), (second_id, 2026, 12345, 7, "ny")],
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}"."{table_name_by_model["GeoZipLookup"]}"(zip_code,state) VALUES($1,$2)',
        "10001",
        "NY",
    )
    selected = await _autocomplete(fixture, zip_code="10001", limit=1)
    assert [plan["state"] for plan in selected["plans"]] == ["NY"]
    replaced = await _autocomplete(fixture, zip_code="10001", limit=2)
    assert [plan["state"] for plan in replaced["plans"]] == ["CA", "NY"]
    assert (await _autocomplete(fixture, zip_code="99999", limit=2)) == replaced
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}"."{table_name_by_model["PlanRatingAreas"]}"(county,zip3,state,market) VALUES($1,$2,$3,$4)',
        "Example",
        "100",
        "NY",
        "Individual",
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}"."{table_name_by_model["ZipState"]}"(zip,stusps) VALUES($1,$2)',
        "10001",
        "NY",
    )
    await fixture.connection.execute(
        f'REVOKE SELECT ON "{fixture.control_schema}"."{table_name_by_model["GeoZipLookup"]}" FROM "{fixture.roles["reader"]}"'
    )
    with pytest.raises(RegistryNetworkCatalogError, match="unavailable"):
        await _autocomplete(fixture, zip_code="10001", limit=1)
    await fixture.connection.execute(
        f'GRANT SELECT ON "{fixture.control_schema}"."{table_name_by_model["GeoZipLookup"]}" TO "{fixture.roles["reader"]}"'
    )
    for table_name in ("GeoZipLookup", "PlanRatingAreas", "ZipState"):
        await fixture.connection.execute(f'DROP TABLE "{fixture.control_schema}"."{table_name_by_model[table_name]}"')
        after = await _autocomplete(fixture, zip_code="10001", limit=1)
        assert [plan["state"] for plan in after["plans"]] == (["CA"] if table_name == "ZipState" else ["NY"])


async def test_native_autocomplete_closed_binding_only_changes_after_approval(catalog_db, monkeypatch):
    fixture = catalog_db
    binding_document = _binding(
        fixture.records.gamma["record_id"], ACA, "autocomplete-close", {**ACA_SCOPE, "checksum_network": 999}
    )
    fixture.manifest = await _publish_legacy_batch_bindings(fixture, [binding_document])
    retained = fixture.manifest
    await _autocomplete_sources(
        fixture,
        monkeypatch,
        [(ACA_SCOPE["plan_id"], 2026, 12345, "CA", "Archive sample")],
        [(ACA_SCOPE["plan_id"], 2026, 12345, 999, "exact")],
    )
    assert len((await _autocomplete(fixture))["plans"]) == 1
    closed = binding_document | {
        "operation": "close",
        "expected_revision": 1,
        "expected_network_id": fixture.records.gamma["record_id"],
    }
    added = await _bind(fixture, [closed])
    assert len((await _autocomplete(fixture))["plans"]) == 1
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *added),
        fixture.seed.actor,
    )
    fixture.manifest = await _publish(fixture, approval["approved_revision"], retained.generation_id)
    assert (await _autocomplete(fixture))["plans"] == []
    assert len((await _autocomplete(fixture, generation=retained.generation_id))["plans"]) == 1
