# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed scope compatibility on maintained named publication read paths."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest
from sanic.exceptions import Forbidden, ServiceUnavailable
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import dialect

from api import custom_import_provider_network as signed
from api import network_address_scope as addresses
from api import network_compact_address as compact
from api import plan_pricing_em_distance as distance
from api import provider_profile_network_read as profiles
from api.network_read_execution import qualified_relation_sql
from db.models import EntityAddressUnified
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import _pinned_manifest
from process.registry_source_recipe_store import RegistrySourceRecipeError
from tests.network_read_execution_support import mock_approved_control_metadata
from tests.test_custom_import_provider_network import _body, _bound_document, _install_manifest, _ScopedSession
from tests.test_network_fhir_retained_capture import pin, retained


def _named_manifest(*, layout=None):
    """Decode the selected logical publication with its genuine named routing DTOs."""
    candidate_id = str(UUID(int=1))
    schema = "network_candidate_" + UUID(candidate_id).hex
    manifest_by_field = dict(
        generation_id=1,
        candidate_id=candidate_id,
        schema_name=schema,
        schema_revision=2 if layout is not None else 1,
        source_generations={"unified_address": "retained-address-edition"},
        approved_custom_revision=0,
        manifest_sha256="a" * 64,
    )
    return _pinned_manifest(
        dict(
            candidate_json={**manifest_by_field, "validation_json": {"serving_readiness": {}}},
            manifest_json=manifest_by_field,
        ),
        layout=layout,
    )


def _layout_manifest(artifacts, custom):
    schema = "network_candidate_" + UUID(int=1).hex
    layout = CompactNetworkLayout(
        CompactAddressRelation(schema, "network_slice_ref"),
        CompactAddressRelation("held_base", "entity_address_unified"),
        tuple(
            CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex)
            for index in range(2, 2 + artifacts)
        ),
        CompactAddressRelation("custom_offices", "entity_address_unified") if custom else None,
    )
    return _named_manifest(layout=layout)


@pytest.mark.parametrize("artifacts,custom", [(0, False), (1, False), (2, True)])
def test_compact_legacy_scope_filters_logical_rows_and_preserves_pins(artifacts, custom):
    manifest = _layout_manifest(artifacts, custom)
    with addresses.network_address_read_scope(manifest, (42,), legacy_checksums=(-17, 42)) as scope:
        raw = addresses.scoped_address_relation_sql("mrf.entity_address_unified")
        typed = addresses.scoped_address_statement(select(EntityAddressUnified.__table__))
        sql = str(typed.compile(dialect=dialect()))
        for statement in (raw, sql):
            assert "plans_network_array &&" in statement
            assert "_legacy_network_checksums" in statement
            assert "WHERE FALSE" not in statement
            assert "ref.network_id=ANY" not in statement
            assert "OR (TRUE)" in statement
        layout = manifest.compact_layout
        expected = (layout.base,) + ((layout.custom,) if custom else ())
        if artifacts:
            expected += (layout.ref_map,) + tuple(
                CompactAddressRelation(artifact.schema_name, "network_office_edge") for artifact in layout.artifacts
            )
        assert compact.compact_address_read_relations(layout) == expected
        for relation in expected:
            qualified = qualified_relation_sql(relation.schema_name, relation.table_name)
            unquoted_table = '"' + relation.schema_name + '".' + relation.table_name
            assert all(qualified in statement or unquoted_table in statement for statement in (raw, sql))
        assert tuple(typed.selected_columns.keys()) == tuple(EntityAddressUnified.__table__.columns.keys())
        parameters = addresses.scoped_address_parameters({})
        assert parameters["_legacy_network_checksums"] == [-17, 42]
        assert parameters["_canonical_network_ids"] == [42]
        assert len([name for name in parameters if name.startswith("_compact_")]) == artifacts
        distance._validate_page_parameters(parameters)
        assert addresses.scoped_address_insurance_predicate() == "NOT (plans_network_array @@ '0'::query_int)"
        assert addresses.scoped_address_aca_insurance_predicate() == "NOT (plans_network_array @@ '0'::query_int)"
        assert scope.legacy_checksums == (-17, 42)


def test_signed_legacy_scope_changes_full_authority_cache_and_read_identity():
    document = _bound_document()
    original = signed.parse_provider_network_binding(document)
    document["network_binding"]["legacy_checksums"] = [-17, 42]
    subset = signed.parse_provider_network_binding(document)
    assert subset.network_ids == original.network_ids
    assert subset.access_scope_sha256 != original.access_scope_sha256
    identities = []
    for checksums in ((-17,), (42,)):
        with addresses.network_address_read_scope(_layout_manifest(1, False), (42,), legacy_checksums=checksums):
            identities.append(
                (
                    addresses.scoped_address_cache_key("page"),
                    addresses.scoped_address_relation_sql("mrf.entity_address_unified"),
                )
            )
            with pytest.raises(ValueError, match="reserved"):
                addresses.scoped_address_parameters({"_legacy_network_checksums": [True]})
    assert identities[0] != identities[1]


@pytest.mark.parametrize("checksums", [None, [], [True], [42, -17], [42, 42], [-2147483649], [2147483648]])
def test_present_signed_legacy_subset_cannot_fall_back_to_wider_canonical_scope(checksums):
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = checksums
    with pytest.raises(signed.CustomImportReadRequestError):
        signed.parse_provider_network_binding(document)


@pytest.mark.parametrize("matches", [True, False])
@pytest.mark.parametrize("checksums", [(-17, 42), (-2147483648, 0, 2147483647)])
async def test_signed_provider_only_acknowledges_enforced_legacy_subset(monkeypatch, matches, checksums):
    from api import custom_import_provider_http as providers
    from tests import test_custom_import_provider_http as page_fixtures

    session = _ScopedSession()
    page_fixtures._install(monkeypatch, session, page_rows=[{"npi": 1104212877}])
    session.driver.delegate = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"complete": matches, "network_ids": [42, 88]})
    )
    _install_manifest(monkeypatch, session)
    original = providers._read_provider_payload

    async def read(*arguments):
        scope = addresses.current_network_address_scope()
        assert scope.legacy_checksums == checksums
        assert "plans_network_array &&" in str(scope.address_relation)
        assert "canonical_network_ids &&" not in str(scope.address_relation)
        return await original(*arguments)

    monkeypatch.setattr(providers, "_read_provider_payload", read)
    document = _bound_document()
    document["network_binding"]["legacy_checksums"] = list(checksums)
    reply = await providers.serve_custom_import_providers(page_fixtures._request(_body(document)), session)
    assert reply.headers["Cache-Control"] == "private, no-store"
    if matches:
        assert reply.status == 200 and reply.headers["X-Network-Legacy-Checksums"] == ",".join(map(str, checksums))
    else:
        assert reply.status == 503 and "prepare" not in session.events
        assert "X-Network-Legacy-Checksums" not in reply.headers
    assert "heap-lock" not in session.events
    assert addresses.current_network_address_scope() is None


@pytest.mark.parametrize("outcome", ["matching", "mismatch", "native"])
async def test_actual_canonical_scope_enforces_mapping_before_handler(monkeypatch, outcome):
    session = _ScopedSession()
    session.events.extend(["begin", "snapshot"])
    delegate = AsyncMock(return_value={"complete": outcome != "mismatch", "network_ids": [42]})
    if outcome == "native":
        delegate.side_effect = asyncpg.InsufficientPrivilegeError("synthetic")
    session.driver.delegate = SimpleNamespace(fetchrow=delegate)
    session.execute = AsyncMock()
    monkeypatch.setattr(addresses, "_network_scope_claim", lambda *_: ((42,), "a" * 64, (-17,), 1))
    monkeypatch.setattr("api.network_serving_read_context.bind_network_request_reader", AsyncMock(return_value=session))

    @asynccontextmanager
    async def snapshot(actual, generation_id=None, control_schema=None):
        assert actual is session and generation_id == 1 and control_schema is None
        yield _named_manifest(), CompactAddressRelation("registry_control", "registry_approved_record")

    monkeypatch.setattr("api.network_serving_api_read.api_catalog_read_snapshot", snapshot)
    mock_approved_control_metadata(monkeypatch)
    reached_checksums = []

    @addresses.canonical_network_read
    async def handler(_request):
        reached_checksums.append(addresses.current_network_address_scope().legacy_checksums)
        return SimpleNamespace(headers={})

    request = SimpleNamespace(query_string="network_ids=42", ctx=SimpleNamespace(sa_session=session))
    if outcome == "matching":
        reply = await handler(request)
        assert reached_checksums == [(-17,)] and reply.headers["X-Network-Legacy-Checksums"] == "-17"
    else:
        with pytest.raises(Forbidden if outcome == "mismatch" else ServiceUnavailable) as caught:
            await handler(request)
        assert reached_checksums == [] and caught.value.headers["Cache-Control"] == "private, no-store"
    assert addresses.current_network_address_scope() is None


def _mapping_driver(events, transaction, failure):
    """Exercise the actual approved legacy map and its native error modes."""

    class Mapping:
        def __getitem__(self, key):
            events.append("decode")
            return {"complete": True, "network_ids": [88] if failure == "mapping" else [42]}[key]

    original = asyncpg.InsufficientPrivilegeError("synthetic") if failure == "native" else asyncio.CancelledError()

    class Driver:
        original_failure = original

        async def fetchrow(self, query, *arguments):
            events.append("mapping")
            assert query.count('JOIN ONLY "registry_control".registry_approved_record') == 2
            assert arguments == (0, ("-17",), "checksum_network", ())
            if failure == "native":
                raise original
            if failure == "cancel":
                raise original
            if failure == "transaction":
                transaction[0] = object()
            return Mapping()

    return Driver()


@pytest.mark.parametrize("failure", [None, "transaction", "mapping", "native", "cancel"])
async def test_actual_mapping_and_original_transaction_precede_payload(monkeypatch, failure):
    """Enforce the signed legacy map before any scoped provider payload is reached."""
    events, transaction = [], [object()]
    approved = CompactAddressRelation("registry_control", "registry_approved_record")

    driver = _mapping_driver(events, transaction, failure)
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: transaction[0])
    session.connection = AsyncMock(
        return_value=SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
        )
    )

    async def approved_pin(actual, manifest):
        assert actual is driver
        events.append("approved")
        return None, approved

    monkeypatch.setattr(
        "process.network_approved_membership_source.pin_api_retained_approved_membership_source", approved_pin
    )
    scope = addresses.network_address_read_session_scope(session, _named_manifest(), (42,), legacy_checksums=(-17,))
    if failure:
        expected = (
            addresses._LegacyNetworkScopeMismatch
            if failure == "mapping"
            else asyncio.CancelledError
            if failure == "cancel"
            else ServiceUnavailable
        )
        with pytest.raises(expected) as caught:
            async with scope:
                pytest.fail("unverified mapping reached the payload")
        if failure != "cancel":
            assert caught.value.headers["Cache-Control"] == "private, no-store"
        if failure == "native":
            assert caught.value.__cause__ is driver.original_failure
        elif failure == "cancel":
            assert caught.value is driver.original_failure
    else:
        async with scope:
            assert events == ["approved", "mapping", "decode", "decode"]
    if failure == "transaction":
        assert events == ["approved", "mapping", "decode", "decode"]
    assert addresses.current_network_address_scope() is None


@pytest.mark.parametrize("kind", ["retained", "live", "recipe"])
async def test_optional_profile_omission_follows_full_api_recipe_authentication(monkeypatch, kind):
    source = pin(retained())
    if kind == "live":
        source = replace(source, retained_capture=None)
    resolver = AsyncMock(return_value=[SimpleNamespace(source_pin=source)])
    if kind == "recipe":
        resolver.side_effect = RegistrySourceRecipeError("registry_source_recipes_unavailable")
    monkeypatch.setattr(profiles, "resolve_retained_registry_source_recipes", resolver)
    from api.endpoint import npi as provider

    observed = AsyncMock(return_value={})
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", observed)
    session = SimpleNamespace(
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=object()))
            )
        )
    )
    with addresses.network_address_read_scope(_named_manifest(), (42,)):
        if kind == "retained":
            assert await profiles.read_detail_directory_profile(session, 1000000491, include_evidence=False) is None
        else:
            with pytest.raises(ServiceUnavailable):
                await profiles.read_detail_directory_profile(session, 1000000491, include_evidence=False)
    assert resolver.await_args.kwargs == {"api_read": True}
    if kind != "live":
        observed.assert_not_awaited()


async def test_unscoped_profile_keeps_mandatory_service_unavailable(monkeypatch):
    from api.endpoint import npi as provider

    monkeypatch.setattr(
        provider, "_fetch_provider_directory_profile_map", AsyncMock(side_effect=ServiceUnavailable("synthetic"))
    )
    session = SimpleNamespace()
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def savepoint(_session):
        yield

    monkeypatch.setattr(profiles, "provider_read_savepoint", savepoint)
    with pytest.raises(ServiceUnavailable):
        await profiles.read_detail_directory_profile(session, 1000000491, include_evidence=False)
