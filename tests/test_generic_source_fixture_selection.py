# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable parent selection preserves the independent UUID child guard."""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from tests import cms_npd_admission_postgres_support as admission
from tests import generic_fhir_capture_postgres_support as retained
from tests import generic_source_successor_postgres_support as source

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "database",
    [
        "hc_network_registry_" + "a" * 32,
        "hc_cms_admission_test_" + "b" * 32,
        "ptg2_v3_lifecycle_test_ci_runner",
    ],
)
@pytest.mark.parametrize("raise_inside", [False, True])
async def test_disposable_parent_uses_uuid_child_and_restores_settings(monkeypatch, database, raise_inside):
    selected = make_url(f"postgresql://tester@127.0.0.1:5440/{database}")
    child = selected.set(drivername="postgresql+asyncpg", database="hc_cms_admission_test_" + "c" * 32)
    original = selected.render_as_string(hide_password=False)
    monkeypatch.setenv(admission._DSN_ENV, original)
    allocation_events = []

    @asynccontextmanager
    async def allocate(parent):
        assert parent == selected.set(drivername="postgresql+asyncpg")
        allocation_events.append("entered")
        try:
            yield child, None
        finally:
            allocation_events.append("closed")

    monkeypatch.setattr(admission, "_owned_database", allocate)
    try:
        async with source._owned_source_database(monkeypatch):
            assert admission._database_url() == child
            assert child.database != selected.database
            if raise_inside:
                raise RuntimeError("synthetic source setup refusal")
    except RuntimeError as error:
        assert raise_inside and str(error) == "synthetic source setup refusal"
    assert os.environ[admission._DSN_ENV] == original
    assert allocation_events == ["entered", "closed"]


@pytest.mark.parametrize(
    "selected",
    [
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 31,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 33,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "A" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "g" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32 + "_suffix",
        "postgresql://tester@127.0.0.1:5440/hc_network_other_" + "a" * 32,
        "postgresql://tester@127.0.0.1:5440/production",
        "postgresql://tester@outside.example:5440/hc_network_registry_" + "a" * 32,
        "postgresql://tester@127.0.0.1/hc_network_registry_" + "a" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32 + "?sslmode=disable",
        "sqlite:///hc_network_registry_" + "a" * 32,
    ],
)
async def test_invalid_parent_refuses_before_database_creation(monkeypatch, selected):
    monkeypatch.setenv(admission._DSN_ENV, selected)

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid parent reached database allocation")

    monkeypatch.setattr(admission, "_owned_database", forbidden)
    with pytest.raises(pytest.fail.Exception, match="disposable local PostgreSQL test database"):
        async with source._owned_source_database(monkeypatch):
            raise AssertionError("invalid parent entered the Source fixture")


async def test_registry_parent_fallback_preserves_missing_cms_setting(monkeypatch):
    selected = make_url("postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32)
    child = selected.set(drivername="postgresql+asyncpg", database="hc_cms_admission_test_" + "b" * 32)
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", selected.render_as_string(hide_password=False))

    @asynccontextmanager
    async def allocate(parent):
        assert parent == selected.set(drivername="postgresql+asyncpg")
        yield child, None

    monkeypatch.setattr(admission, "_owned_database", allocate)
    async with source._owned_source_database(monkeypatch):
        assert admission._database_url() == child
    assert admission._DSN_ENV not in os.environ


async def test_missing_selection_skips_without_allocation(monkeypatch):
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.delenv("NETWORK_REGISTRY_TEST_DSN", raising=False)

    def forbidden(*args, **kwargs):
        raise AssertionError("unconfigured fixture reached database allocation")

    monkeypatch.setattr(admission, "_owned_database", forbidden)
    with pytest.raises(pytest.skip.Exception, match="explicitly selected disposable PostgreSQL database"):
        async with source._owned_source_database(monkeypatch):
            raise AssertionError("unconfigured fixture entered the Source fixture")


def _capture_fixture_connections(events):
    """Expose only a source reader and the existing administrator preparation connection."""

    @asynccontextmanager
    async def transaction(kind, **options):
        events.append((kind, options))
        try:
            yield
        except BaseException:
            events.append((kind, "rollback"))
            raise
        else:
            events.append((kind, "commit"))

    reader = SimpleNamespace(transaction=lambda **options: transaction("reader", **options))
    admin = SimpleNamespace(transaction=lambda **options: transaction("admin", **options))

    async def connect(kind):
        assert kind == "reader", "capture connected a limited preparation publisher"
        return reader

    return SimpleNamespace(
        admin=admin, connect=connect, captures=[], origin=object(), role_by_kind={"owner": "owner", "reader": "reader"}
    ), reader


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("custom_options", [False, True])
async def test_retained_capture_uses_admin_and_independent_read_snapshot(monkeypatch, failure, custom_options):
    events = []
    fixture, reader = _capture_fixture_connections(events)
    origin = object() if custom_options else None
    roles = ("additional_reader", "reader") if custom_options else None
    capture_result = object()

    async def capture(actual_reader, writer, actual_origin, **options):
        assert actual_reader is reader and writer is fixture.admin
        assert actual_origin is (origin or fixture.origin)
        assert options["owner_role"] == "owner" and options["runtime_roles"] == (roles or ("reader",))
        assert type(options["capture_id"]) is UUID
        assert fixture.captures == ["registry_fhir_capture_" + options["capture_id"].hex]
        assert events == [
            ("reader", {"isolation": "repeatable_read", "readonly": True}),
            ("admin", {"isolation": "repeatable_read"}),
        ]
        if failure is not None:
            raise failure("capture failed")
        return capture_result

    prepare = AsyncMock(side_effect=capture)
    monkeypatch.setattr(retained, "capture_retained_fhir_source", prepare)
    if failure is None:
        assert await retained._capture(fixture, origin, runtime_roles=roles) is capture_result
    else:
        with pytest.raises(failure, match="capture failed"):
            await retained._capture(fixture, origin, runtime_roles=roles)
    prepare.assert_awaited_once()
    outcome = "rollback" if failure else "commit"
    assert events[-2:] == [("admin", outcome), ("reader", outcome)]


async def test_source_continuation_configures_independent_reader_and_admin_writer(monkeypatch):
    connect = Mock(side_effect=AssertionError("fixture configuration connected to PostgreSQL"))
    monkeypatch.setattr(AsyncEngine, "connect", connect)
    admin_url = make_url("postgresql+asyncpg://administrator@127.0.0.1:5440/disposable")
    factory = SimpleNamespace(_require_current_policy=Mock(), _native_input_producer=object())
    component = SimpleNamespace(factory=factory, admin_engine=SimpleNamespace(url=admin_url), reader="source_reader")
    fixture = SimpleNamespace(role_by_kind={"owner": "capture_owner"})
    async with AsyncExitStack() as cleanup:
        await source._continuation(component, fixture, cleanup, monkeypatch)
        installed = factory.generic_fhir_capture_continuation
        reader_engine = installed.reader_factory.kw["bind"]
        writer_engine = installed.writer_factory.kw["bind"]
        assert reader_engine is not writer_engine
        assert reader_engine.url == admin_url.set(username="source_reader")
        assert writer_engine.url == admin_url
        assert installed.target.owner_role == "capture_owner"
        assert installed.target.runtime_roles == ("source_reader",)
    factory._require_current_policy.assert_called_once_with()
    connect.assert_not_called()


@pytest.mark.parametrize("resource_name", ["provider_directory_dataset_resource", "named_resource_payload"])
async def test_approved_capture_recipe_preserves_named_resource_relation(monkeypatch, resource_name):
    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.network_fhir_retained_capture_contract import capture_digest
    from tests.test_network_fhir_retained_capture import retained as capture_fixture

    capture = capture_fixture()
    origin = replace(capture.origin, resource_relation={"schema": capture.origin.schema_name, "name": resource_name})
    capture = replace(capture, origin=origin)
    capture = replace(capture, capture_sha256=capture_digest(capture))
    fixture = SimpleNamespace(origin=origin, role_by_kind={"reader": "source_reader"})
    transaction_options = []

    @asynccontextmanager
    async def transaction(**options):
        transaction_options.append(options)
        yield

    connection = SimpleNamespace(transaction=transaction, fetchval=AsyncMock(return_value=3), execute=AsyncMock())
    approved = ApprovedMembershipSource(3, "d" * 64)
    monkeypatch.setattr(retained, "_draft", AsyncMock(return_value={"record_id": 7}))
    monkeypatch.setattr(retained, "apply_network_source_binding_batch", AsyncMock(return_value={"records": [{}]}))
    monkeypatch.setattr(retained, "_command", AsyncMock(return_value={}))
    monkeypatch.setattr(retained, "_approve", AsyncMock())
    monkeypatch.setattr(retained, "pin_approved_membership_source", AsyncMock(return_value=approved))
    recipe, actual_approved, network_id = await retained._approved_capture_recipe(
        fixture, (connection, "registry", object()), capture
    )
    assert actual_approved is approved and network_id == 7
    assert recipe.source_pin.resource_relation == origin.resource_relation
    assert recipe.source_pin.retained_capture == capture
    assert recipe.source_pin.read_resource_relation == capture.tables["provider_directory_dataset_resource"]
    assert recipe.binding_coordinates.sql_parameters[:4] == (
        "fhir",
        origin.source_id,
        origin.schema_name,
        origin.dataset_id,
    )
    assert recipe.binding_coordinates.edition_id == capture.capture_id
    assert transaction_options == [{}, {"isolation": "repeatable_read"}]


@pytest.mark.parametrize("named", [False, True])
async def test_source_fixture_selects_named_enrollment_only_when_requested(monkeypatch, named):
    selection_events = []
    sentinel = object()

    @asynccontextmanager
    async def selected():
        selection_events.append(named)
        yield sentinel
        selection_events.append("cleaned")

    monkeypatch.setattr(source, "_named_generic_source", lambda _: selected() if named else None)
    monkeypatch.setattr(source, "_legacy_generic_source", lambda _: selected() if not named else None)
    async with source.native_generic_source(monkeypatch, named=named) as actual:
        assert actual is sentinel and selection_events == [named]
    assert selection_events == [named, "cleaned"]


@pytest.mark.parametrize("edition", [1, 2])
async def test_named_http_edition_preserves_office_and_provider_facts(edition):
    pages_by_kind = source._edition_fhir_resources(edition)
    normalized_by_kind = {}
    for kind, page_resources in pages_by_kind.items():
        normalized_by_kind[kind] = [
            source.directory.materialize_provider_directory_dataset_fhir_resource(
                source_id=source.SOURCE,
                dataset_id="dataset",
                resource={"resourceType": kind, **resource},
                run_id="run",
                semantic_projection_as_of="2026-10-01",
                resource_hash_contract=source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            )["payload_json"]
            for resource in page_resources
        ]
    offices_by_id = {payload["resource_id"]: payload for payload in normalized_by_kind["Location"]}
    assert {key: payload["second_line"] for key, payload in offices_by_id.items()} == {
        "office-one": "Suite 1",
        "office-two": "Suite 2",
    }
    roles_by_id = {payload["resource_id"]: payload for payload in normalized_by_kind["PractitionerRole"]}
    office_role = roles_by_id["role-office"]
    assert office_role["location_refs"] == ["Location/office-one" if edition == 1 else "Location/office-two"]
    assert office_role["network_refs"] == ["Organization/network-one"] and office_role["insurance_plan_refs"] == []
    provider_role = roles_by_id["role-provider-only-" + str(edition)]
    assert provider_role["npi"] == (1000000004 if edition == 1 else 1000000491)
    assert provider_role["location_refs"] == [] and provider_role["network_refs"] == ["Organization/network-one"]
