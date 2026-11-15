# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API billing cursors use logical publication generations and named relations."""

import asyncio
import dataclasses
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from api import billing_search_pagination as pagination
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests import test_billing_search_pagination as fixture

_SNAPSHOT_NAMES = ["mrf.entity_address_evidence", "mrf.entity_address_unified"]


def _session(*, generation=1, modes=("repeatable read", "on"), snapshot_names=_SNAPSHOT_NAMES):
    """Reuse names pinned before the request's first data snapshot."""
    transaction = object()
    return SimpleNamespace(
        get_transaction=lambda: transaction,
        in_transaction=lambda: True,
        info={"provider_profile_relations": {name: name for name in snapshot_names}},
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(one=lambda: modes),
                SimpleNamespace(one_or_none=lambda: ("11111111-1111-4111-8111-111111111111", generation)),
            ]
        ),
    )


async def test_api_pin_matches_named_logical_generation_bundle():
    session = _session()
    pin = await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert pin == fixture._pin()
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 2 and statements[1].startswith("SELECT origin_lineage_id")
    assert "pg_catalog" not in "".join(statements)
    assert set(dataclasses.asdict(pin)) == {
        "snapshot_set_sha256",
        "generation_bundle_sha256",
        "address_relation_name",
        "address_evidence_relation_name",
    }


async def test_changed_logical_publication_expires_the_previous_bundle():
    first = await pagination.capture_billing_search_generation_pin(_session(), fixture._selection(), api_read=True)
    second = await pagination.capture_billing_search_generation_pin(
        _session(generation=2), fixture._selection(), api_read=True
    )
    assert first.snapshot_set_sha256 == second.snapshot_set_sha256
    assert first.generation_bundle_sha256 != second.generation_bundle_sha256


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_api_pin_requires_read_only_repeatable_transaction(modes):
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(
            _session(modes=modes), fixture._selection(), api_read=True
        )


async def test_cancellation_reaches_the_transaction_owner():
    failure = asyncio.CancelledError()
    session = _session()
    session.execute.side_effect = failure
    with pytest.raises(asyncio.CancelledError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert caught.value is failure


async def test_generation_metadata_sql_failure_uses_the_normal_unavailable_error():
    session = _session()
    failure = DBAPIError(None, None, RuntimeError("missing metadata"))
    session.execute.side_effect = failure
    with pytest.raises(PTG2ManifestArtifactError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert caught.value.__cause__ is failure


def _legacy_session(
    *,
    local_generation=0,
    lineage="11111111-1111-4111-8111-111111111111",
    origin=(None, None),
    modes=("repeatable read", "on"),
):
    session = _session()

    async def execute(statement, parameters=None):
        sql = str(statement)
        if "current_setting" in sql:
            return SimpleNamespace(one=lambda: modes)
        if "SELECT origin_lineage_id" in sql:
            return SimpleNamespace(one_or_none=lambda: origin)
        if "SELECT local_lineage_id" in sql:
            return SimpleNamespace(one_or_none=lambda: (lineage, local_generation))
        raise AssertionError(sql)

    session.execute.side_effect = execute
    return session


@pytest.mark.parametrize("api_read", [False, True])
async def test_legacy_adoption_serves_first_page_with_pinned_names_and_local_edition(api_read):
    session = _legacy_session()
    pin = await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=api_read)
    request = fixture._request()
    binding = fixture._cursor_binding(request=request, pin=pin)
    assert pagination.open_billing_search_page_cursor(request, keyring=fixture.KEYRING, binding=binding) is None
    assert pin.address_relation_name == "mrf.entity_address_unified"
    assert pin.address_evidence_relation_name == "mrf.entity_address_evidence"
    assert len(session.execute.await_args_list) == 3
    assert all(str(call.args[0]).startswith("SELECT ") for call in session.execute.await_args_list)


@pytest.mark.parametrize("change", ["successor", "lineage", "origin"])
async def test_legacy_cursor_refuses_another_logical_edition(change):
    previous = await pagination.capture_billing_search_generation_pin(_legacy_session(), fixture._selection())
    previous_binding = fixture._cursor_binding(pin=previous)
    sealed = pagination.seal_billing_search_page_cursor((1,), keyring=fixture.KEYRING, binding=previous_binding)
    request = fixture._request(cursor=fixture._wire_token(sealed, previous_binding))
    replacement = (
        _legacy_session(local_generation=1)
        if change == "successor"
        else _legacy_session(lineage="22222222-2222-4222-8222-222222222222")
        if change == "lineage"
        else _session()
    )
    current = await pagination.capture_billing_search_generation_pin(replacement, fixture._selection(), api_read=True)
    assert current.snapshot_set_sha256 == previous.snapshot_set_sha256
    assert current.generation_bundle_sha256 != previous.generation_bundle_sha256
    with pytest.raises(fixture.cursor.BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(
            request, keyring=fixture.KEYRING, binding=fixture._cursor_binding(request=request, pin=current)
        )


@pytest.mark.parametrize("generation", [None, -1, True, 2**63])
async def test_legacy_pin_refuses_invalid_local_edition(generation):
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(
            _legacy_session(local_generation=generation), fixture._selection()
        )


@pytest.mark.parametrize("origin", [(None, 1), ("11111111-1111-4111-8111-111111111111", None)])
async def test_partial_source_origin_cannot_be_reinterpreted_as_legacy(origin):
    session = _legacy_session(origin=origin)
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection())
    assert session.execute.await_count == 1


async def test_legacy_pin_requires_snapshot_even_without_explicit_api_option():
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(
            _legacy_session(modes=("read committed", "on")), fixture._selection()
        )


async def test_legacy_named_read_refusal_remains_unavailable(monkeypatch):
    from api import provider_profile_snapshot

    session = _legacy_session()
    failure = DBAPIError(None, None, RuntimeError("read refused"))
    monkeypatch.setattr(provider_profile_snapshot, "_prepare_provider_reader", AsyncMock(side_effect=failure))
    with pytest.raises(PTG2ManifestArtifactError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection())
    assert caught.value.__cause__ is failure


async def test_cutover_after_snapshot_start_fails_before_any_address_read():
    session = _session(snapshot_names=["mrf.entity_address_evidence", "mrf.entity_address_unified_stage"])
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.execute.await_count == 1


async def test_unprepared_borrowed_snapshot_is_refused_before_generation_read():
    session = _session()
    session.info.clear()
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.execute.await_count == 1


def _bundle_issued_before_upgrade(address_oid, evidence_oid):
    """The previous version's bundle, written out independently of the current helpers."""
    encoded = json.dumps(
        {
            "address_relation_oid": address_oid,
            "address_evidence_relation_oid": evidence_oid,
            "address_selection_contract": pagination.BILLING_ADDRESS_SELECTION_CONTRACT,
            "snapshot_set_sha256": pagination.billing_search_snapshot_set_sha256(fixture._selection()),
        },
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256(b"HEALTHPORTA_BILLING_SEARCH_GENERATION_BUNDLE_V1\x00")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


async def test_cursor_without_logical_generation_expires_and_a_new_cursor_continues():
    previous_binding = dataclasses.replace(
        fixture._cursor_binding(), generation_bundle_sha256=_bundle_issued_before_upgrade(101, 102)
    )
    sealed = pagination.seal_billing_search_page_cursor((1,), keyring=fixture.KEYRING, binding=previous_binding)
    request = fixture._request(cursor=fixture._wire_token(sealed, previous_binding))
    session = _session()
    current = await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    binding = fixture._cursor_binding(request=request, pin=current)
    assert binding.generation_bundle_sha256 != previous_binding.generation_bundle_sha256
    with pytest.raises(fixture.cursor.BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(request, keyring=fixture.KEYRING, binding=binding)
    next_cursor = pagination.seal_billing_search_page_cursor((2,), keyring=fixture.KEYRING, binding=binding)
    next_request = fixture._request(cursor=fixture._wire_token(next_cursor, binding))
    assert pagination.open_billing_search_page_cursor(next_request, keyring=fixture.KEYRING, binding=binding) == (2,)
