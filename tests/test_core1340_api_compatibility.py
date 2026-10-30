# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Semantic compatibility only; native custody remains independently required."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest
from sanic.exceptions import Forbidden, ServiceUnavailable
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import dialect

from api import custom_import_provider_network as signed
from api import network_address_scope as addresses
from api import network_read_execution as execution
from api import plan_pricing_em_distance as distance
from api import provider_profile_network_read as profiles
from api.provider_directory_entities_contract import DirectoryReadError
from db.models import EntityAddressUnified
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_read_identity import _CONSUMED_SQL
from process.registry_source_recipe_store import RegistrySourceRecipeError
from tests.network_read_execution_support import mock_approved_control_metadata
from tests.test_custom_import_provider_network import _body, _bound_document, _install_manifest, _ScopedSession
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_network_read_execution import _manifest


def _layout_manifest(artifacts, custom):
    manifest = _manifest(compact=True)
    layout = CompactNetworkLayout(
        manifest.compact_layout.ref_map,
        manifest.compact_layout.base,
        tuple(
            CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex, 30 + index)
            for index in range(2, 2 + artifacts)
        ),
        CompactAddressRelation("custom_offices", "entity_address_unified", 90) if custom else None,
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    return manifest


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
        raw_pins = execution.network_read_relations(text("SELECT * FROM " + raw + " office"))
        assert execution.network_read_relations(typed) == raw_pins
        assert len(raw_pins) == 1 + bool(custom) + (1 + artifacts if artifacts else 0)
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
        with addresses.network_address_read_scope(_manifest(compact=True), (42,), legacy_checksums=checksums):
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
    monkeypatch.setattr(addresses, "_pin_network_scope", AsyncMock(return_value=(_manifest(), (42,), "a" * 64, (-17,))))
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
    """Keep exact native mapping/consumed ordering and injected failure modes."""

    class Mapping:
        def __getitem__(self, key):
            events.append("decode")
            return {"complete": True, "network_ids": [88] if failure == "mapping" else [42]}[key]

    class Driver:
        def get_server_pid(self):
            return 42

        def is_in_transaction(self):
            return True

        async def fetchrow(self, query, *arguments):
            if query == _CONSUMED_SQL:
                events.append("guard")
                assert arguments[0] == ['"registry_control"."registry_approved_record"']
                return {"backend_matches": True, "relation_count": 1, "valid": failure != "guard"}
            events.append("mapping")
            assert query.count('JOIN ONLY "registry_control".registry_approved_record') == 2
            assert arguments == (0, ("-17",), "checksum_network", ())
            if failure == "native":
                raise asyncpg.InsufficientPrivilegeError("synthetic")
            if failure == "cancel":
                raise asyncio.CancelledError()
            if failure == "transaction":
                transaction[0] = object()
            return Mapping()

    return Driver()


@pytest.mark.parametrize("failure", [None, "guard", "transaction", "mapping", "native", "cancel"])
async def test_actual_mapping_guard_and_original_context_precede_decoding(monkeypatch, failure):
    """Consume native mapping under the original transaction before exposing decoded fields."""
    events, transaction = [], [object()]
    approved = CompactAddressRelation("registry_control", "registry_approved_record", 99)

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
    scope = addresses.network_address_read_session_scope(session, _manifest(), (42,), legacy_checksums=(-17,))
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
    else:
        async with scope:
            assert events == ["approved", "mapping", "guard", "decode", "decode"]
    if failure in ("guard", "transaction"):
        assert events == ["approved", "mapping", "guard"]
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
    with addresses.network_address_read_scope(_manifest(), (42,)):
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
