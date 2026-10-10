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
from process.registry_management_permissions import (
    install_registry_management_permissions,
    verify_registry_management_permissions,
)
from process.registry_network_catalog_read import (
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacySelector,
    RegistryNetworkCatalogQuery,
    read_registry_network_catalog,
    read_registry_network_catalog_detail,
    resolve_registry_network_catalog_legacy,
)
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command, _create
from tests.test_registry_candidate_composition_postgres import (
    _remove_candidates,
    _roles,
)
from tests.test_registry_company_links_postgres import _links

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
