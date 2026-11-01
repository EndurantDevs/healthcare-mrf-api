# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy membership stays independent from its authorized canonical identity."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic import response
from sanic.compat import Header
from sanic.exceptions import Forbidden, InvalidUsage
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from api import network_address_scope as scope
from db.models import EntityAddressUnified
from process import network_serving_read
from process.network_serving_read import PinnedNetworkServingManifest
from tests.network_read_execution_support import mock_approved_control_metadata, with_read_identity_receipts

MANIFEST = PinnedNetworkServingManifest(
    7,
    "00000000-0000-0000-0000-000000000001",
    "network_candidate_00000000000000000000000000000001",
    1,
    {},
    4,
    "a" * 64,
    81,
)


def request(checksums=None, *, query="network_ids=42&network_generation=7", authorized=True):
    headers = Header({"X-Network-Access-Scope": "b" * 64})
    if authorized:
        headers["Authorization"] = "Bearer synthetic-legacy-token"
    if checksums is not None:
        headers["X-Network-Legacy-Checksums"] = checksums
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value={"complete": True, "network_ids": [42]}))
    session = with_read_identity_receipts(SimpleNamespace(execute=AsyncMock()), driver=driver)
    driver.native = session.connection.return_value.get_raw_connection.return_value.driver_connection
    return SimpleNamespace(headers=headers, query_string=query, ctx=SimpleNamespace(sa_session=session)), driver


@pytest.fixture(autouse=True)
def approved_metadata(monkeypatch):
    mock_approved_control_metadata(monkeypatch)


def test_legacy_predicate_keeps_authorization_but_does_not_require_canonical_office_coverage():
    keys = set()
    for checksums in (None, (42,), (84,), (42, 84)):
        with scope.network_address_read_scope(
            MANIFEST, (42,), access_scope_sha256="b" * 64, legacy_checksums=checksums
        ):
            statement = scope.scoped_address_statement(select(EntityAddressUnified.__table__))
            sql = str(statement.compile(dialect=postgresql.dialect()))
            where = sql.split("WHERE", 1)[1]
            raw = scope.scoped_address_relation_sql("mrf.entity_address_unified")
            parameters = scope.scoped_address_parameters({})
            assert parameters["_canonical_network_ids"] == [42]
            if checksums is None:
                assert "canonical_network_ids &&" in where and "plans_network_array &&" not in where
            else:
                assert "plans_network_array &&" in where and "canonical_network_ids &&" not in where
                assert "plans_network_array &&" in raw and "canonical_network_ids &&" not in raw
                assert parameters["_legacy_network_checksums"] == list(checksums)
                with pytest.raises(ValueError, match="reserved_parameter"):
                    scope.scoped_address_parameters({"_legacy_network_checksums": [99]})
            keys.add(scope.scoped_address_cache_key("same-query"))
    assert len(keys) == 4 and scope.current_network_address_scope() is None


@pytest.mark.parametrize("checksums", [(), [42], (True,), (42, 42), (84, 42), (-2147483649,), (2147483648,)])
def test_invalid_checksum_scope_is_rejected(checksums):
    with pytest.raises(ValueError, match="legacy_scope_invalid"):
        with scope.network_address_read_scope(MANIFEST, (42,), legacy_checksums=checksums):
            pytest.fail("invalid selector installed")


@pytest.mark.parametrize("raw", ["", "42,42", "84,42", "042", "+42", "-0", "2147483648", "42,", "4.2"])
async def test_invalid_header_never_reaches_mapping_or_native(monkeypatch, raw):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    incoming, driver = request(raw)
    with pytest.raises(InvalidUsage):
        await scope._pin_network_scope(incoming, scope._request_selector(incoming, None))
    driver.fetchrow.assert_not_awaited()
    incoming.ctx.sa_session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "mapping",
    [
        None,
        {"complete": False, "network_ids": [42]},
        {"complete": True, "network_ids": [84]},
        {"complete": True, "network_ids": [42, 84]},
    ],
)
async def test_missing_ambiguous_or_mismatched_mapping_refuses_before_handler(monkeypatch, mapping):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", AsyncMock(return_value=MANIFEST))
    incoming, driver = request("42")
    driver.fetchrow.return_value = mapping
    handler = AsyncMock(return_value=response.json({"unexpected": True}))
    with pytest.raises(Forbidden) as refusal:
        await scope.canonical_network_read(handler)(incoming)
    assert refusal.value.headers["Cache-Control"] == "private, no-store"
    handler.assert_not_awaited()
    assert driver.fetchrow.await_args.args[1:] == (4, ("42",), "checksum_network", ())


@pytest.mark.parametrize("checksums", [None, "-2147483648,0,42,2147483647"])
async def test_verified_header_is_exact_pinned_scope_and_acknowledged(monkeypatch, checksums):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    pin = AsyncMock(return_value=MANIFEST)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", pin)
    incoming, driver = request(checksums)

    async def native(_request):
        selected = scope.current_network_address_scope()
        assert selected.network_ids == (42,) and selected.manifest is MANIFEST
        assert selected.legacy_checksums == (tuple(map(int, checksums.split(","))) if checksums else None)
        return response.json({"rows": []})

    result = await scope.canonical_network_read(native)(incoming)
    assert result.headers.get("X-Network-Legacy-Checksums") == checksums
    assert result.headers["X-Network-Generation"] == "7"
    assert scope.current_network_address_scope() is None
    pin.assert_awaited_once_with(driver.native, generation_id=7, api_read=True)
    assert driver.fetchrow.await_count == int(checksums is not None)


async def test_nested_batch_read_reuses_the_outer_legacy_scope(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    pin = AsyncMock(return_value=MANIFEST)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", pin)
    incoming, driver = request("42")

    @scope.canonical_network_read
    async def nested(_request, *, native_args):
        assert native_args == {"npi": "1000000000"}
        selected = scope.current_network_address_scope()
        assert selected.manifest is MANIFEST and selected.network_ids == (42,)
        assert selected.legacy_checksums == (42,)
        return response.json({"rows": []})

    @scope.canonical_network_read
    async def batch(_request):
        return await nested(_request, native_args={"npi": "1000000000"})

    result = await batch(incoming)
    assert result.headers["X-Network-Generation"] == "7"
    assert result.headers["X-Network-Legacy-Checksums"] == "42"
    pin.assert_awaited_once_with(driver.native, generation_id=7, api_read=True)
    assert driver.fetchrow.await_count == 1
    assert scope.current_network_address_scope() is None


async def test_public_header_cannot_replace_control_authority(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    incoming, driver = request("42", authorized=False)
    incoming.ctx._sa_session_token = object()
    handler = AsyncMock()
    with pytest.raises(Forbidden):
        await scope.canonical_network_read(handler)(incoming)
    handler.assert_not_awaited()
    driver.fetchrow.assert_not_awaited()
    incoming.ctx.sa_session.execute.assert_not_awaited()


@pytest.mark.parametrize("query", ["plan_network=42", "network_ids=42&checksum=42", "network_ids=42&network_ids=84"])
def test_header_does_not_admit_missing_or_mixed_public_namespaces(query):
    incoming, _ = request("42", query=query)
    with pytest.raises(InvalidUsage):
        scope._request_selector(incoming, None)


@pytest.mark.parametrize("missing", ["pin", "access", "duplicate"])
async def test_header_requires_one_value_and_both_trusted_pins(monkeypatch, missing):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    incoming, driver = request("42")
    if missing == "pin":
        incoming.query_string = "network_ids=42"
    elif missing == "access":
        del incoming.headers["X-Network-Access-Scope"]
    else:
        incoming.headers.add("X-Network-Legacy-Checksums", "84")
    with pytest.raises(InvalidUsage):
        await scope._pin_network_scope(incoming, scope._request_selector(incoming, None))
    driver.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "active,reader_scope,current_owner,configure",
    [(False, None, True, True), (True, None, True, False), (False, object(), True, False), (False, None, False, False)],
)
async def test_http_snapshot_setup_leaves_active_and_unowned_sessions_unchanged(
    monkeypatch, active, reader_scope, current_owner, configure
):
    from db import connection

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-legacy-token")
    pin = AsyncMock(return_value=MANIFEST)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", pin)
    incoming, driver = request()
    session = incoming.ctx.sa_session
    session.in_transaction = lambda: active
    incoming.ctx._sa_session_token = object()
    incoming.ctx._sa_reader_scope = reader_scope
    monkeypatch.setattr(connection, "current_session", lambda: session if current_owner else object())
    await scope._pin_network_scope(incoming, scope._request_selector(incoming, None))
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    expected = ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"] if configure else []
    assert statements == expected + ["SET LOCAL lock_timeout='1s'"]
    pin.assert_awaited_once_with(driver.native, generation_id=7, api_read=True)
